import logging
from typing import Any, Dict

from shared.yaml import UserYaml
from test_results_parser import Outcome

from app import celery_app
from database.enums import ReportType
from database.models import Commit, CommitReport, Test, TestInstance, Upload
from services.lock_manager import LockManager, LockRetry, LockType
from services.test_results import TestResultsNotifier
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)

test_results_finisher_task_name = "app.tasks.test_results.TestResultsFinisherTask"


class TestResultsFinisherTask(BaseCodecovTask, name=test_results_finisher_task_name):
    async def run_async(
        self,
        db_session,
        chord_result: Dict[str, Any],
        *,
        repoid: int,
        commitid: str,
        commit_yaml: dict,
        **kwargs,
    ):
        repoid = int(repoid)
        commit_yaml = UserYaml.from_dict(commit_yaml)

        log.info(
            "Starting test results finisher task",
            extra=dict(
                repoid=repoid,
                commit=commitid,
                commit_yaml=commit_yaml,
            ),
        )

        lock_manager = LockManager(
            repoid=repoid,
            commitid=commitid,
            report_type=ReportType.COVERAGE,
            lock_timeout=max(80, self.hard_time_limit_task),
        )

        try:
            with lock_manager.locked(
                LockType.NOTIFICATION,
                retry_num=self.request.retries,
            ):
                return await self.process_async_within_lock(
                    db_session=db_session,
                    repoid=repoid,
                    commitid=commitid,
                    commit_yaml=commit_yaml,
                    previous_result=chord_result,
                    **kwargs,
                )
        except LockRetry as retry:
            self.retry(max_retries=5, countdown=retry.countdown)

    async def process_async_within_lock(
        self,
        *,
        db_session,
        repoid: int,
        commitid: str,
        commit_yaml: UserYaml,
        previous_result: Dict[str, Any],
        **kwargs,
    ):
        log.info(
            "Running test results finishers",
            extra=dict(
                repoid=repoid,
                commit=commitid,
                commit_yaml=commit_yaml,
                parent_task=self.request.parent_id,
            ),
        )

        commit: Commit = (
            db_session.query(Commit).filter_by(repoid=repoid, commitid=commitid).first()
        )
        assert commit, "commit not found"

        notify = True

        if all(
            (
                testrun_list["successful"] is False
                for result in previous_result
                for testrun_list in result
            )
        ):
            # every processor errored, nothing to notify on
            return {"notify_attempted": False, "notify_succeeded": False}

        test_dict = dict()
        testrun_list = []

        existing_tests = db_session.query(Test).filter(Test.repoid == repoid)

        existing_test_instances = (
            db_session.query(TestInstance)
            .join(Upload)
            .join(CommitReport)
            .join(Commit)
            .filter(Commit.id_ == commit.id_)
            .all()
        )

        for test in existing_tests:
            test_dict[hash((test.testsuite, test.name))] = test

        existing_test_instance_by_test = {
            testrun.test.id: testrun for testrun in existing_test_instances
        }

        for result in previous_result:
            for testrun_dict_list in result:
                if testrun_dict_list["successful"]:
                    for testrun in testrun_dict_list["testrun_list"]:
                        test_hash = hash((testrun["testsuite"], testrun["name"]))
                        if test_hash not in test_dict:
                            test = Test(
                                repoid=repoid,
                                name=testrun["name"],
                                testsuite=testrun["testsuite"],
                                env=testrun_dict_list["env"],
                            )
                            db_session.add(test)
                            test_dict[test_hash] = test
                        else:
                            test = test_dict[test_hash]

                        if test.id in existing_test_instance_by_test:
                            existing_run_number = existing_test_instance_by_test[
                                test.id
                            ].upload.build_code
                            try:
                                if int(testrun_dict_list["run_number"]) > int(
                                    existing_run_number
                                ):
                                    existing_test_instance_by_test[
                                        test.id
                                    ].upload_id = testrun_dict_list["upload_id"]
                                    existing_test_instance_by_test[
                                        test.id
                                    ].duration_seconds = testrun["duration_seconds"]
                                    existing_test_instance_by_test[
                                        test.id
                                    ].outcome = testrun["outcome"]
                                    existing_test_instance_by_test[
                                        test.id
                                    ].failure_message = testrun["failure_message"]

                            except ValueError:
                                pass
                        else:
                            ti = TestInstance(
                                test_id=test.id,
                                test=test,
                                upload_id=testrun_dict_list["upload_id"],
                                duration_seconds=testrun["duration_seconds"],
                                outcome=testrun["outcome"],
                                failure_message=testrun["failure_message"],
                            )
                            db_session.add(ti)
                            testrun_list.append(ti)
        db_session.flush()

        testrun_list += existing_test_instance_by_test.values()

        if all([instance.outcome != Outcome.Failure for instance in testrun_list]):
            return {"notify_attempted": False, "notify_succeeded": False}

        success = None
        notifier = TestResultsNotifier(commit, commit_yaml, testrun_list)
        success = await notifier.notify()

        log.info(
            "Finished test results notify",
            extra=dict(
                repoid=repoid,
                commit=commitid,
                commit_yaml=commit_yaml,
                parent_task=self.request.parent_id,
            ),
        )

        return {"notify_attempted": notify, "notify_succeeded": success}


RegisteredTestResultsFinisherTask = celery_app.register_task(TestResultsFinisherTask())
test_results_finisher_task = celery_app.tasks[RegisteredTestResultsFinisherTask.name]
