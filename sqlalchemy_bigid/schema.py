from sqlalchemy import DDL, event

from sqlalchemy_bigid.utils import get_bigid_column_from_table


create_function_nextbigid_text = """
CREATE OR REPLACE FUNCTION nextbigid(seq_name text, OUT result bigint) AS $$
DECLARE
    -- 1/1/2018
    our_epoch bigint := 1514764800000;
    seq_id bigint;
    now_millis bigint;
    shard_id int := 0;
BEGIN
    SELECT nextval(seq_name) %% 1024 INTO seq_id;

    SELECT FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) INTO now_millis;
    result := (now_millis - our_epoch) << 20;
    result := result | (shard_id << 10);
    result := result | (seq_id);
END;
$$ LANGUAGE PLPGSQL;
"""

create_function_nextbigid = DDL(create_function_nextbigid_text)


def register_nextbigid_function(metadata):
    """
    Create the nextbigid function on initial table creation (mostly for dev)
    """
    event.listen(metadata, 'before_create', create_function_nextbigid)


def generate_nextbigid_sql_for_table(table):
    """
    If a Table has a BigID column, return the Alter table SQL to use nextbigid()
    """
    bigid_column = get_bigid_column_from_table(table)
    if bigid_column is not None:
        return generate_nextbigid_sql(table.name, bigid_column.key)


def generate_nextbigid_sql(table_name, column_name):
    if table_name == 'user':
        # user is a reserved word, need parentheses
        sql = """ALTER TABLE "user" ALTER COLUMN {column} set default nextbigid('user_{column}_seq')""".format(
            column=column_name
        )
    else:
        sql = "ALTER TABLE {table} ALTER COLUMN {column} set default nextbigid('{table}_{column}_seq')".format(
            table=table_name,
            column=column_name,
        )
    return sql


def setup_bigid_for_all_tables(metadata):
    """
    This is more for Base.create_all() usage than for migrations, but still important for that flow.
    Alembic migrations have a different flow
    """
    tables = metadata.sorted_tables
    for table in tables:
        nextbigid_sql = generate_nextbigid_sql_for_table(table)
        if nextbigid_sql:
            alter_table_bigid = DDL(nextbigid_sql)
            event.listen(table, 'after_create', alter_table_bigid)
