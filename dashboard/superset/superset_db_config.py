#This files sets up permissions for the trino database
#Such as being able to create views and tables from it
#Also connects to the database if not already done

import os
os.environ.setdefault('SUPERSET_CONFIG_PATH', '/app/superset_config.py')

from superset import create_app

app = create_app()
with app.app_context():
    from superset.extensions import db
    from superset.models.core import Database

    existing = db.session.query(Database).filter_by(
        database_name='Lakehouse (Trino)'
    ).first()

    if existing:
        existing.expose_in_sqllab  = True
        existing.allow_ctas        = True
        existing.allow_cvas        = True
        existing.allow_dml         = True
        existing.extra             = '{"cost_estimate_enabled": false, "disable_data_preview": false, "expand_rows": false}'
        db.session.commit()
        print('✅ Trino database connection updated')
    else:
        database = Database(
            database_name  = 'Lakehouse (Trino)',
            sqlalchemy_uri = 'trino://admin@trino:8080/lakehouse',
            expose_in_sqllab = True,
            allow_ctas       = True,
            allow_cvas       = True,
            allow_dml        = True,
            extra            = '{"cost_estimate_enabled": false, "disable_data_preview": false, "expand_rows": false}'
        )
        db.session.add(database)
        db.session.commit()
        print('✅ Trino database connection created')