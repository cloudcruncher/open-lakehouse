"""Create or reconcile the Lakehouse connection, so no one has to add it by hand.

Every query runs as the signed-in colleague (their own OAuth2 token), over TLS to Trino.
"""

import json

from superset.app import create_app

app = create_app()

with app.app_context():
    from superset import db
    from superset.models.core import Database

    desired = {
        "sqlalchemy_uri": "trino://trino:8443/lakehouse",
        "impersonate_user": True,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_dml": False,
        "allow_file_upload": False,
        "extra": json.dumps(
            {
                "engine_params": {
                    "connect_args": {"http_scheme": "https", "verify": "/etc/ssl/lakehouse/ca.pem"}
                },
            }
        ),
    }
    database = db.session.query(Database).filter_by(database_name="Lakehouse").one_or_none()
    if database is None:
        database = Database(database_name="Lakehouse")
        db.session.add(database)
    for key, value in desired.items():
        setattr(database, key, value)
    db.session.commit()

    # Superset's own gate: SQL Lab users may pick this connection. What they can read
    # inside it is decided by Trino and OPA, per colleague.
    from superset import security_manager as sm

    pv = sm.add_permission_view_menu("database_access", database.perm)
    sm.add_permission_role(sm.find_role("sql_lab"), pv)
    db.session.commit()
    print("superset: Lakehouse connection ensured (queries run as the signed-in colleague)")
