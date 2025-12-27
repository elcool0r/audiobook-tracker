import warnings


def _suppress_utcnow_warning(message, category, filename, lineno, file=None, line=None):
    msg = str(message)
    if category is DeprecationWarning and "datetime.datetime.utcnow()" in msg:
        return
    _original_showwarning(message, category, filename, lineno, file=file, line=line)


_original_showwarning = warnings.showwarning
warnings.showwarning = _suppress_utcnow_warning
