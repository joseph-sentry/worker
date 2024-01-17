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
from tasks.notify import notify_task_name

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

        if self.check_if_no_success(previous_result):
            # every processor errored, nothing to notify on
            return {"notify_attempted": False, "notify_succeeded": False}

        testrun_list = []

        test_dict = self.get_test_dict(db_session, repoid)

        existing_test_instance_by_test = self.get_existing_test_instance_by_test(
            db_session, commit
        )

        for result in previous_result:
            # finish_individual_result
            for testrun_dict_list in result:
                if testrun_dict_list["successful"]:
                    for testrun in testrun_dict_list["testrun_list"]:
                        testsuite = testrun["testsuite"]
                        name = testrun["name"]
                        env = testrun_dict_list["env"]
                        run_number = testrun_dict_list["run_number"]
                        upload_id = testrun_dict_list["upload_id"]
                        duration_seconds = testrun["duration_seconds"]
                        outcome = testrun["outcome"]
                        failure_message = testrun["failure_message"]

                        test = self.get_or_create_test(
                            db_session, test_dict, testsuite, name, repoid, env
                        )

                        # always create a new test instance
                        ti = TestInstance(
                            test_id=test.id,
                            upload_id=upload_id,
                            duration_seconds=duration_seconds,
                            outcome=outcome,
                            failure_message=failure_message,
                            active=True,
                        )
                        db_session.add(ti)
                        db_session.flush()

                        # if there exists a test instance that maps to the same test as the one we are
                        # currently examining, we check if we should overwrite it

                        # If we should, we set the old test instance to inactive and replace it with the new
                        # test instance in the existing_test_instance_by_test dictionary, so we have the newest
                        # test instance we know of in the dict
                        if test.id in existing_test_instance_by_test:
                            should_overwrite = self.should_overwrite_test_instance(
                                existing_test_instance_by_test, test.id, run_number
                            )
                            if should_overwrite is True:
                                existing_test_instance_by_test[test.id].active = False
                                db_session.flush()
                                existing_test_instance_by_test[test.id] = ti
                            else:
                                ti.active = False
                                db_session.flush()

                        else:
                            testrun_list.append(ti)

        testrun_list += existing_test_instance_by_test.values()

        if self.check_if_no_failures(testrun_list):
            self.app.tasks[notify_task_name].apply_async(
                args=None,
                kwargs=dict(
                    repoid=repoid, commitid=commitid, current_yaml=commit_yaml.to_dict()
                ),
            )
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

    def check_if_no_success(self, previous_result):
        return all(
            (
                testrun_list["successful"] is False
                for result in previous_result
                for testrun_list in result
            )
        )

    def get_or_create_test(self, db_session, test_dict, testsuite, name, repoid, env):
        test_hash = hash((testsuite, name, env))
        if test_hash not in test_dict:
            test = Test(
                repoid=repoid,
                name=name,
                testsuite=testsuite,
                env=env,
            )
            db_session.add(test)
            db_session.flush()
            test_dict.update({test_hash: test})
        else:
            test = test_dict.get(test_hash)

        return test

    def should_overwrite_test_instance(
        self,
        test_map,
        test_id,
        run_number,
    ):
        existing_run_number = test_map[test_id].upload.build_code
        try:
            if int(run_number) > int(existing_run_number):
                return True
            else:
                return False
        # This error happens if the run_number or existing_run_number are not
        # convertible to integers
        except TypeError:
            pass
        return True

    def check_if_no_failures(self, testrun_list):
        return all(
            [instance.outcome != int(Outcome.Failure) for instance in testrun_list]
        )

    def get_existing_test_instance_by_test(self, db_session, commit):
        # we only care about the existing active test instances
        # the inactive ones are out of date anyways
        existing_test_instances = (
            db_session.query(TestInstance)
            .join(Upload)
            .join(CommitReport)
            .join(Commit)
            .filter(Commit.id_ == commit.id_, TestInstance.active == True)
            .all()
        )

        return {testrun.test.id: testrun for testrun in existing_test_instances}

    def get_test_dict(self, db_session, repoid):
        existing_tests = db_session.query(Test).filter(Test.repoid == repoid)
        return {
            hash((test.testsuite, test.name, test.env)): test for test in existing_tests
        }


RegisteredTestResultsFinisherTask = celery_app.register_task(TestResultsFinisherTask())
test_results_finisher_task = celery_app.tasks[RegisteredTestResultsFinisherTask.name]
