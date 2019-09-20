from timestring import Date

from services.yaml import read_yaml_field
from covreports.resources import Report, ReportFile
from covreports.utils.tuples import ReportLine

from helpers.exceptions import ReportExpiredException
from services.report.languages.base import BaseLanguageProcessor


class CoberturaProcessor(BaseLanguageProcessor):

    def matches_content(self, content, first_line, name):
        return True  # TODO Fix this

    def process(self, name, content, path_fixer, ignored_lines, sessionid, repo_yaml):
        return from_xml(content, path_fixer, ignored_lines, sessionid, repo_yaml)


def Int(value):
    try:
        return int(value)
    except Exception:
        return int(float(value))


def from_xml(xml, fix, ignored_lines, sessionid, yaml):
    # # process timestamp
    if read_yaml_field(yaml, ('codecov', 'max_report_age'), '12h ago'):
        try:
            timestamp = next(xml.iter('coverage')).get('timestamp')
        except StopIteration:
            try:
                timestamp = next(xml.iter('scoverage')).get('timestamp')
            except StopIteration:
                timestamp = None
        if timestamp and Date(timestamp) < read_yaml_field(yaml, ('codecov', 'max_report_age'), '12h ago'):
            # report expired over 12 hours ago
            raise ReportExpiredException("Cobertura report expired " + timestamp)

    report = Report()

    for _class in xml.iter('class'):
        filename = _class.attrib['filename']
        _file = ReportFile(filename)

        for line in _class.iter('line'):
            _line = line.attrib
            ln = _line['number']
            if ln == 'undefined':
                continue
            ln = int(ln)
            if ln > 0:
                coverage = None
                _type = None
                sessions = None

                # coverage
                branch = _line.get('condition-coverage', '')
                if branch and branch != 'NaN':
                    coverage = branch.split(' ', 1)[1][1:-1]  # 1/2
                    _type = 'b'
                else:
                    coverage = Int(_line.get('hits'))

                # [python] [scoverage] [groovy] Conditions
                conditions = _line.get('missing-branches', None)
                if conditions:
                    conditions = conditions.split(',')
                    if len(conditions) > 1 and set(conditions) == set(('exit', )):
                        # python: "return [...] missed"
                        conditions = ['loop', 'exit']
                    sessions = [[sessionid, coverage, conditions]]

                else:
                    # [groovy] embedded conditions
                    conditions = ['%(number)s:%(type)s' % _.attrib
                                  for _ in line.iter('condition')
                                  if _.attrib.get('coverage') != '100%']
                    if type(coverage) is str and coverage[0] == '0' and len(conditions) < int(coverage.split('/')[1]):
                        # <line number="23" hits="0" branch="true" condition-coverage="0% (0/2)">
                        #     <conditions>
                        #         <condition number="0" type="jump" coverage="0%"/>
                        #     </conditions>
                        # </line>
                        conditions.extend(map(str, range(len(conditions), int(coverage.split('/')[1]))))
                    if conditions:
                        sessions = [[sessionid, coverage, conditions]]
                _file.append(ln,
                             ReportLine(coverage=coverage,
                                        type=_type,
                                        sessions=sessions or [[sessionid, coverage]]))

        # [scala] [scoverage]
        for stmt in _class.iter('statement'):
            # scoverage will have repeated data
            stmt = stmt.attrib
            if stmt.get('ignored') == 'true':
                continue
            coverage = Int(stmt['invocation-count'])
            if stmt['branch'] == 'true':
                _file.append(int(stmt['line']), ReportLine(coverage, 'b', [[sessionid, coverage]]))
            else:
                _file.append(int(stmt['line']), ReportLine(coverage, 'm' if stmt['method'] else None, [[sessionid, coverage]]))
        report.append(_file)

    # path rename
    path_name_fixing = []
    for _class in xml.iter('class'):
        filename = _class.attrib['filename']
        path_name_fixing.append((filename, fix(filename)))

    _set = set(('dist-packages', 'site-packages'))
    report.resolve_paths(sorted(path_name_fixing,
                                key=lambda a: _set & set(a[0].split('/'))))

    report.ignore_lines(ignored_lines)
    return report
