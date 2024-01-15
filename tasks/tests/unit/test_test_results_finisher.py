from pathlib import Path

import pytest
from mock import AsyncMock
from shared.torngit.exceptions import TorngitClientError
from test_results_parser import Outcome

from database.enums import ReportType
from database.models import CommitReport, Test, TestInstance
from database.tests.factories import CommitFactory, PullFactory, UploadFactory
from services.repository import EnrichedPull
from tasks.test_results_finisher import TestResultsFinisherTask

here = Path(__file__)


class TestUploadTestFinisherTask(object):
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_upload_finisher_task_call(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        upload = UploadFactory.create()
        dbsession.add(upload)
        dbsession.flush()

        mocker.patch.object(TestResultsFinisherTask, "app", celery_app)

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="joseph-sentry",
            repository__owner__service="github",
            repository__name="codecov-demo",
        )

        pull = PullFactory.create(repository=commit.repository, head=commit.commitid)

        _ = mocker.patch(
            "services.test_results.fetch_and_update_pull_request_information_from_commit",
            return_value=EnrichedPull(
                database_pull=pull,
                provider_pull={},
            ),
        )
        m = mocker.MagicMock(
            edit_comment=AsyncMock(return_value=True),
            post_comment=AsyncMock(return_value={"id": 1}),
        )
        mocked_repo_provider = mocker.patch(
            "services.test_results.get_repo_provider_service",
            return_value=m,
        )

        dbsession.add(commit)
        dbsession.flush()
        current_report_row = CommitReport(
            commit_id=commit.id_, report_type=ReportType.TEST_RESULTS.value
        )
        dbsession.add(current_report_row)
        dbsession.flush()
        upload.report = current_report_row
        dbsession.flush()

        result = await TestResultsFinisherTask().run_async(
            dbsession,
            [
                [
                    {
                        "successful": True,
                        "upload_id": upload.id,
                        "env": "",
                        "run_number": None,
                        "testrun_list": [
                            {
                                "duration_seconds": 0.001,
                                "name": "api.temp.calculator.test_calculator::test_add",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.001,
                                "name": "api.temp.calculator.test_calculator::test_subtract",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.0,
                                "name": "api.temp.calculator.test_calculator::test_multiply",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.001,
                                "name": "hello world",
                                "outcome": int(Outcome.Failure),
                                "testsuite": "hello world testsuite",
                                "failure_message": "bad failure",
                            },
                        ],
                    }
                ],
            ],
            repoid=upload.report.commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": False}},
        )
        expected_result = {"notify_attempted": True, "notify_succeeded": True}
        m.post_comment.assert_called_with(
            pull.pullid,
            "##  [Codecov](url) Report\n\n**Test Failures Detected**: Due to failing tests, we cannot provide coverage reports at this time.\n\n### :x: Failed Test Results: \nCompleted 4 tests with **`1 failed`**, 3 passed and 0 skipped.\n<details><summary>View the full list of failed tests</summary>\n\n| **File path** | **Failure message** |\n| :-- | :-- |\n| hello world testsuite::hello world | <pre>bad failure</pre> |",
        )
        assert expected_result == result

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_upload_finisher_task_call_no_failures(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        upload = UploadFactory.create()
        dbsession.add(upload)
        dbsession.flush()

        mocker.patch.object(TestResultsFinisherTask, "app", celery_app)

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="joseph-sentry",
            repository__owner__service="github",
            repository__name="codecov-demo",
        )
        dbsession.add(commit)
        dbsession.flush()
        current_report_row = CommitReport(
            commit_id=commit.id_, report_type=ReportType.TEST_RESULTS.value
        )
        dbsession.add(current_report_row)
        dbsession.flush()
        upload.report = current_report_row
        dbsession.flush()

        result = await TestResultsFinisherTask().run_async(
            dbsession,
            [
                [
                    {
                        "successful": True,
                        "upload_id": upload.id,
                        "env": "",
                        "run_number": None,
                        "testrun_list": [
                            {
                                "duration_seconds": 0.001,
                                "name": "api.temp.calculator.test_calculator::test_add",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.001,
                                "name": "api.temp.calculator.test_calculator::test_subtract",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.0,
                                "name": "api.temp.calculator.test_calculator::test_multiply",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.001,
                                "name": "hello world",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "hello world testsuite",
                                "failure_message": None,
                            },
                        ],
                    }
                ],
            ],
            repoid=upload.report.commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": False}},
        )
        expected_result = {"notify_attempted": False, "notify_succeeded": False}

        assert expected_result == result

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_upload_finisher_task_call_no_successful(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        upload = UploadFactory.create()
        dbsession.add(upload)
        dbsession.flush()

        mocker.patch.object(TestResultsFinisherTask, "app", celery_app)

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="joseph-sentry",
            repository__owner__service="github",
            repository__name="codecov-demo",
        )
        dbsession.add(commit)
        dbsession.flush()
        current_report_row = CommitReport(
            commit_id=commit.id_, report_type=ReportType.TEST_RESULTS.value
        )
        dbsession.add(current_report_row)
        dbsession.flush()
        upload.report = current_report_row
        dbsession.flush()

        test = Test(
            repoid=upload.report.commit.repoid,
            name="hello world",
            testsuite="hello world testsuite",
            env="",
        )

        test_instance = TestInstance(
            test_id=test.id,
            duration_seconds=1,
            outcome=int(Outcome.Pass),
            failure_message=None,
            upload_id=upload.id,
        )

        dbsession.add(test)
        dbsession.add(test_instance)
        dbsession.flush()

        result = await TestResultsFinisherTask().run_async(
            dbsession,
            [
                [{"successful": False}],
            ],
            repoid=upload.report.commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": False}},
        )
        expected_result = {"notify_attempted": False, "notify_succeeded": False}

        assert expected_result == result

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_upload_finisher_task_call_post_comment_fails(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        upload = UploadFactory.create()
        dbsession.add(upload)
        dbsession.flush()

        mocker.patch.object(TestResultsFinisherTask, "app", celery_app)

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="joseph-sentry",
            repository__owner__service="github",
            repository__name="codecov-demo",
        )

        pull = PullFactory.create(repository=commit.repository, head=commit.commitid)

        _ = mocker.patch(
            "services.test_results.fetch_and_update_pull_request_information_from_commit",
            return_value=EnrichedPull(
                database_pull=pull,
                provider_pull={},
            ),
        )
        mocked_repo_provider = mocker.patch(
            "services.test_results.get_repo_provider_service",
            return_value=mocker.MagicMock(
                edit_comment=AsyncMock(return_value=True),
                post_comment=AsyncMock(side_effect=TorngitClientError),
            ),
        )

        dbsession.add(commit)
        dbsession.flush()
        current_report_row = CommitReport(
            commit_id=commit.id_, report_type=ReportType.TEST_RESULTS.value
        )
        dbsession.add(current_report_row)
        dbsession.flush()
        upload.report = current_report_row
        dbsession.flush()

        result = await TestResultsFinisherTask().run_async(
            dbsession,
            [
                [
                    {
                        "successful": True,
                        "env": "",
                        "run_number": None,
                        "upload_id": upload.id,
                        "testrun_list": [
                            {
                                "duration_seconds": 0.001,
                                "name": "api.temp.calculator.test_calculator::test_add",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.001,
                                "name": "api.temp.calculator.test_calculator::test_subtract",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.0,
                                "name": "api.temp.calculator.test_calculator::test_multiply",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.001,
                                "name": "hello world",
                                "outcome": int(Outcome.Failure),
                                "testsuite": "hello world testsuite",
                                "failure_message": "bad failure",
                            },
                        ],
                    }
                ],
            ],
            repoid=upload.report.commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": False}},
        )
        expected_result = {"notify_attempted": True, "notify_succeeded": False}
        assert expected_result == result

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_upload_finisher_task_call_edit_comment(
        self,
        mocker,
        mock_configuration,
        dbsession,
        codecov_vcr,
        mock_storage,
        mock_redis,
        celery_app,
    ):
        upload = UploadFactory.create()
        dbsession.add(upload)
        dbsession.flush()

        mocker.patch.object(TestResultsFinisherTask, "app", celery_app)

        commit = CommitFactory.create(
            message="hello world",
            commitid="cd76b0821854a780b60012aed85af0a8263004ad",
            repository__owner__unencrypted_oauth_token="test7lk5ndmtqzxlx06rip65nac9c7epqopclnoy",
            repository__owner__username="joseph-sentry",
            repository__owner__service="github",
            repository__name="codecov-demo",
        )

        pull = PullFactory.create(
            repository=commit.repository, head=commit.commitid, commentid=1, pullid=1
        )

        _ = mocker.patch(
            "services.test_results.fetch_and_update_pull_request_information_from_commit",
            return_value=EnrichedPull(
                database_pull=pull,
                provider_pull={},
            ),
        )

        mocked_repo_provider = mocker.patch(
            "services.test_results.get_repo_provider_service",
        )
        m = mocker.MagicMock()
        m.edit_comment = AsyncMock()

        mocked_repo_provider.return_value = m

        dbsession.add(commit)
        dbsession.flush()
        current_report_row = CommitReport(
            commit_id=commit.id_, report_type=ReportType.TEST_RESULTS.value
        )
        dbsession.add(current_report_row)
        dbsession.flush()
        upload.report = current_report_row
        dbsession.flush()

        result = await TestResultsFinisherTask().run_async(
            dbsession,
            [
                [
                    {
                        "successful": True,
                        "env": "",
                        "run_number": None,
                        "upload_id": upload.id,
                        "testrun_list": [
                            {
                                "duration_seconds": 0.001,
                                "name": "api.temp.calculator.test_calculator::test_add",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.001,
                                "name": "api.temp.calculator.test_calculator::test_subtract",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.0,
                                "name": "api.temp.calculator.test_calculator::test_multiply",
                                "outcome": int(Outcome.Pass),
                                "testsuite": "pytest",
                                "failure_message": None,
                            },
                            {
                                "duration_seconds": 0.001,
                                "name": "hello world",
                                "outcome": int(Outcome.Failure),
                                "testsuite": "hello world testsuite",
                                "failure_message": "bad failure",
                            },
                        ],
                    }
                ],
            ],
            repoid=upload.report.commit.repoid,
            commitid=commit.commitid,
            commit_yaml={"codecov": {"max_report_age": False}},
        )
        expected_result = {"notify_attempted": True, "notify_succeeded": True}

        m.edit_comment.assert_called_with(
            pull.pullid,
            1,
            "##  [Codecov](url) Report\n\n**Test Failures Detected**: Due to failing tests, we cannot provide coverage reports at this time.\n\n### :x: Failed Test Results: \nCompleted 4 tests with **`1 failed`**, 3 passed and 0 skipped.\n<details><summary>View the full list of failed tests</summary>\n\n| **File path** | **Failure message** |\n| :-- | :-- |\n| hello world testsuite::hello world | <pre>bad failure</pre> |",
        )

        assert expected_result == result
