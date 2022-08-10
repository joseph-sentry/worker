import typing

from shared.reports.resources import Report, ReportFile
from shared.reports.types import ReportLine

from services.report.languages.base import BaseLanguageProcessor
from services.report.report_builder import ReportBuilder


class RspecProcessor(BaseLanguageProcessor):
    def matches_content(self, content, first_line, name):
        return isinstance(content, dict) and content.get("command_name") == "RSpec"

    def process(
        self, name: str, content: typing.Any, report_builder: ReportBuilder
    ) -> Report:
        path_fixer, ignored_lines, sessionid, repo_yaml = (
            report_builder.path_fixer,
            report_builder.ignored_lines,
            report_builder.sessionid,
            report_builder.repo_yaml,
        )
        return from_json(content, path_fixer, ignored_lines, sessionid)


def from_json(json, fix, ignored_lines, sessionid):
    report = Report()
    for data in json["files"]:
        fn = fix(data["filename"])
        if fn is None:
            continue

        _file = ReportFile(fn, ignore=ignored_lines.get(fn))

        # Structure depends on which Simplecov version was used so we need to handle either structure
        coverage = data["coverage"]
        coverage_to_check = (
            coverage["lines"]
            if isinstance(coverage, dict)
            and coverage.get("lines")  # Simplecov version >= 0.18
            else coverage  # Simplecov version < 0.18
        )

        for ln, cov in enumerate(coverage_to_check, start=1):
            _file[ln] = ReportLine.create(coverage=cov, sessions=[[sessionid, cov]])

        report.append(_file)

    return report
