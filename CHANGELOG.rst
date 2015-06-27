Changelog
=========

Version 0.2
-----------

Unreleased.

- Add decorator functionality.
- Fix nested maintain_schema with different sessions.
- Fix nested maintain_schema with scoped_session proxies.
- Add oracle support.
- Don't try (and fail) to restore the schema if the session is in a rollback state.
- Don't swallow handled exception in the context manager's exit.

Version 0.1
-----------

Released on 2015-05-31.

- First release.
