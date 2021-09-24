from enum import Enum


class Decoration(Enum):
    standard = "standard"
    upgrade = "upgrade"


class Notification(Enum):
    comment = "comment"
    status_changes = "status_changes"
    status_patch = "status_patch"
    status_project = "status_project"
    checks_changes = "checks_changes"
    checks_patch = "checks_patch"
    checks_project = "checks_project"
    slack = "slack"
    webhook = "webhook"
    gitter = "gitter"
    irc = "irc"
    hipchat = "hipchat"


class NotificationState(Enum):
    pending = "pending"
    success = "success"
    error = "error"


class CompareCommitState(Enum):
    pending = "pending"
    processed = "processed"
    error = "error"
