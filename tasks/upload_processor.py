from json import loads
from time import time
import logging
import re
import zlib

import minio.error
from celery import exceptions
from covreports.utils.sessions import Session

from app import celery_app
from database.models import Commit
from helpers.config import get_config
from helpers.exceptions import ReportExpiredException
from services.archive import ArchiveService
from services.bots import RepositoryWithoutValidBotError
from services.redis import get_redis_connection
from services.report import ReportService
from services.repository import get_repo_provider_service
from services.yaml import read_yaml_field
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)

regexp_ci_skip = re.compile(r'\[(ci|skip| |-){3,}\]').search
merged_pull = re.compile(r'.*Merged in [^\s]+ \(pull request \#(\d+)\).*').match


class UnableToTestException(Exception):
    pass


class UploadProcessorTask(BaseCodecovTask):
    """This is the second task of the series of tasks designed to process an `upload` made
    by the user

    To see more about the whole picture, see `tasks.upload.UploadTask`

    This task processes each user `upload`, and saves the results to db and minio storage

    The steps are:
        - Fetching the user uploaded report (from minio, or sometimes redis)
        - Running them through the language processors, and obtaining reports from that
        - Merging the generated reports to the already existing commit processed reports
        - Saving all that info to the database

    This task doesn't limit how many individual reports it receives for processing. It deals
        with as many as possible. But it is not expected that this task will receive a big
        number of `uploads` to be processed
    """
    name = "app.tasks.upload_processor.UploadProcessorTask"

    def write_to_db(self):
        return False

    def schedule_for_later_try(self):
        retry_in = 20 * (self.request.retries + 1)
        self.retry(max_retries=3, countdown=retry_in)

    async def run_async(self, db_session, previous_results, *, repoid, commitid, commit_yaml, arguments_list, **kwargs):
        processings_so_far = previous_results.get('processings_so_far', [])
        repoid = int(repoid)
        log.debug("In run_async for repoid %d and commit %s", repoid, commitid)
        lock_name = f"upload_processing_lock_{repoid}_{commitid}"
        redis_connection = get_redis_connection()
        with redis_connection.lock(lock_name, timeout=60 * 5, blocking_timeout=30):
            log.debug("Obtained lock for repoid %d and commit %s", repoid, commitid)
            commit = None
            n_processed = 0
            commits = db_session.query(Commit).filter(
                    Commit.repoid == repoid, Commit.commitid == commitid)
            commit = commits.first()
            assert commit, 'Commit not found in database.'
            repository = commit.repository
            try:
                repository_service = get_repo_provider_service(repository, commit)
            except RepositoryWithoutValidBotError:
                log.exception(
                    'Unable to process report because there is no valid bot found for that repo',
                    extra=dict(
                        repoid=repoid,
                        commit=commitid,
                        arguments_list=arguments_list,
                        commit_yaml=commit_yaml
                    )
                )
                raise
            pr = None
            should_delete_archive = self.should_delete_archive(commit_yaml)
            try_later = []
            archive_service = ArchiveService(repository)
            try:
                report = ReportService().build_report_from_commit(commit)
            except Exception:
                log.exception(
                    "Unable to fetch current report for commit",
                    extra=dict(
                        repoid=repoid,
                        commit=commitid,
                        arguments_list=arguments_list,
                        commit_yaml=commit_yaml
                    )
                )
                raise
            try:
                for arguments in arguments_list:
                    pr = arguments.get('pr')
                    log.info(
                        "Processing individual report %s", arguments.get('reportid'),
                        extra=dict(repoid=repoid, commit=commitid, arguments=arguments)
                    )
                    individual_info = {
                        'arguments': arguments.copy()
                    }
                    try:
                        arguments_commitid = arguments.pop('commit', None)
                        if arguments_commitid:
                            assert arguments_commitid == commit.commitid
                        result = self.process_individual_report(
                            archive_service, redis_connection, commit_yaml,
                            commit, report, should_delete_archive, **arguments
                        )
                        individual_info.update(result)
                    except exceptions.CeleryError:
                        raise
                    except Exception:
                        log.exception(
                            "Unable to process report %s", arguments.get('reportid'),
                            extra=dict(
                                commit_yaml=commit_yaml,
                                repoid=repoid,
                                commit=commitid,
                                arguments=arguments
                            )
                        )
                        self.schedule_for_later_try()
                    if individual_info.get('successful'):
                        report = individual_info.pop('report')
                        n_processed += 1
                    processings_so_far.append(individual_info)
                if n_processed > 0:
                    log.info(
                        'Finishing the processing of %d reports',
                        n_processed,
                        extra=dict(repoid=repoid, commit=commitid)
                    )
                    await self.save_report_results(
                        db_session, archive_service, repository_service,
                        repository, commit, report, pr
                    )
                    log.info(
                        'Processed %d reports',
                        n_processed,
                        extra=dict(repoid=repoid, commit=commitid)
                    )
                return {
                    'processings_so_far': processings_so_far,
                }
            except exceptions.CeleryError:
                raise
            except Exception:
                commit.state = 'error'
                log.exception(
                    'Could not properly process commit',
                    extra=dict(
                        repoid=repoid,
                        commit=commitid,
                        arguments=try_later
                    )
                )
                raise

    def process_individual_report(
            self, archive_service, redis_connection, commit_yaml,
            commit, report, *args, **arguments):
        try:
            result = self.do_process_individual_report(
                archive_service, redis_connection, commit_yaml, commit, report, *args, **arguments
            )
            return {
                'successful': True,
                'report': result
            }
        except ReportExpiredException:
            return {
                'successful': False,
                'report': None,
                'error_type': 'report_expired',
                'should_retry': False
            }
        except UnableToTestException:
            log.info(
                "Unable to process report %s due to inherent test conditions",
                arguments.get('reportid'),
                exc_info=True,
                extra=dict(
                    repoid=commit.repoid,
                    commit=commit.commitid,
                    arguments=arguments
                )
            )
            return {
                'successful': False,
                'report': None,
                'error_type': 'unable_to_test',
                'should_retry': False
            }

    def do_process_individual_report(
            self, archive_service, redis_connection,
            commit_yaml, commit, current_report, should_delete_archive, *,
            flags=None, service=None, build_url=None,
            build=None, job=None, name=None, url=None,
            redis_key=None, reportid=None, **kwargs):
        """Takes a `current_report (Report)`, runs a raw_uploaded_report (str) against
            it and generates a new report with the result
        """
        raw_uploaded_report = None
        flags = (flags.split(',') if flags else None)

        archive_url = url
        raw_uploaded_report = self.fetch_raw_uploaded_report(
            archive_service, redis_connection, archive_url, commit.commitid, reportid, redis_key)
        log.debug('Retrieved report for processing from url %s', archive_url)
        # TODO: Remove this once we are not on test mode anymore
        if redis_key and not raw_uploaded_report:
            raise UnableToTestException("Unable to test due to the report living on redis")

        # delete from archive is desired
        if should_delete_archive and archive_url and not archive_url.startswith('http'):
            archive_service.delete_file(archive_url)
            archive_url = None

        # ---------------
        # Process Reports
        # ---------------
        session = Session(
            provider=service,
            build=build,
            job=job,
            name=name,
            time=int(time()),
            flags=flags,
            archive=archive_url or url,
            url=build_url
        )
        report = self.process_raw_upload(
            commit_yaml=commit_yaml,
            master=current_report,
            reports=raw_uploaded_report,
            flags=flags,
            session=session
        )

        log.info(
            'Successfully processed report',
            extra=dict(
                session=session.id,
                ci=f'{session.provider}:{session.build}:{session.job}',
                repoid=commit.repoid,
                commit=commit.commitid,
                reportid=reportid,
                commit_yaml=commit_yaml
            )
        )
        return report

    def process_raw_upload(self, commit_yaml, master, reports, flags, session=None):
        return ReportService().build_report_from_raw_content(
            commit_yaml, master, reports, flags, session
        )

    def fetch_raw_uploaded_report(
            self, archive_service, redis_connection, archive_url, commit_sha, reportid, redis_key):
        """
            Downloads the raw report, wherever it is (it's either a path on minio or redis)

        Args:
            archive_service: [description]
            redis_connection: [description]
            archive_url: [description]
            commit_sha: [description]
            reportid: [description]
            redis_key: [description]
        """
        log.debug("In fetch_raw_uploaded_report for commit: %s" % commit_sha)
        if archive_url:
            try:
                return archive_service.read_file(archive_url)
            except minio.error.NoSuchKey:
                log.exception(
                    "File could not be found on %s for commit %s", archive_url, commit_sha
                )
                raise
        else:
            return self.download_archive_from_redis(archive_service, redis_connection, redis_key)

    def download_archive_from_redis(self, archive_service, redis_connection, redis_key):
        # download from redis
        raw_uploaded_report = redis_connection.get(redis_key)
        gzipped = redis_key.endswith('/gzip')
        # TODO Reconsider deleting the file from redis after testing mode is done
        if gzipped:
            raw_uploaded_report = zlib.decompress(
                raw_uploaded_report, zlib.MAX_WBITS | 16
            )
        if raw_uploaded_report is not None:
            return raw_uploaded_report.decode()
        return None

    def should_delete_archive(self, commit_yaml):
        return False
        if get_config('services', 'minio', 'expire_raw_after_n_days'):
            return True
        return not read_yaml_field(
            commit_yaml,
            ('codecov', 'archive', 'uploads'),
            _else=True
        )

    async def save_report_results(
            self, db_session, archive_service,
            repository_service, repository, commit, report, pr):
        log.debug("In save_report_results for commit: %s" % commit)
        commitid = commit.commitid
        report.apply_diff(await repository_service.get_commit_diff(commitid))

        write_archive_service = ArchiveService(commit.repository, bucket='testingarchive')
        totals, network_json_str = report.to_database()
        network = loads(network_json_str)

        if pr is not None:
            commit.pullid = pr

        commit.state = 'complete' if report else 'error'
        commit.totals = totals
        commit.report = network

        # ------------------------
        # Archive Processed Report
        # ------------------------
        archive_data = report.to_archive().encode()
        url = write_archive_service.write_chunks(commit.commitid, archive_data)
        log.info(
            'Archived report',
            extra=dict(
                repoid=commit.repoid,
                commit=commit.commitid,
                url=url
            )
        )
        _, report_dict = report.to_database()
        return {}


RegisteredUploadTask = celery_app.register_task(UploadProcessorTask())
upload_processor_task = celery_app.tasks[RegisteredUploadTask.name]
