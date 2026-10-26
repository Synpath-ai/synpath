"""Replace nominal parent attribution with captured execution membership.

Migration definitions are frozen. Missing historical accounts require an
explicit mapping in meta['legacy_execution_accounts'] keyed by parent ID,
then venue. Never substitute the current process's configured accounts.
"""
import json

from alembic import op
import sqlalchemy as sa

revision = "0006_routed_execution_scope"
down_revision = "0005_drop_fair_values"
branch_labels = None
depends_on = None
NAMESPACE = "__managed__"


def account_key(account):
    return f"{account['venue']}:{account.get('name', 'default')}" + (
        f":{account['subaccount']}" if account.get('subaccount') else "")


def upgrade():
    connection = op.get_bind()
    op.create_table("parent_execution_accounts",
        sa.Column("parent_id", sa.Text, sa.ForeignKey("managed_orders.id"), primary_key=True),
        sa.Column("venue", sa.Text, primary_key=True),
        sa.Column("account_key", sa.Text, nullable=False))
    op.create_index("parent_execution_venue", "parent_execution_accounts", ["venue", "parent_id"])
    op.create_index("parent_execution_account", "parent_execution_accounts", ["account_key", "parent_id"])
    metadata = sa.MetaData()
    parents = sa.Table("managed_orders", metadata, autoload_with=connection)
    orders = sa.Table("orders", metadata, autoload_with=connection)
    intents = sa.Table("intents", metadata, autoload_with=connection)
    membership = sa.Table("parent_execution_accounts", metadata, autoload_with=connection)
    meta = sa.Table("meta", metadata, autoload_with=connection)
    supplied = connection.execute(sa.select(meta.c.value).where(meta.c.key == "legacy_execution_accounts")).scalar()
    mappings = json.loads(supplied) if supplied else {}
    historical_intent_accounts = {}
    for payload in connection.execute(sa.select(intents.c.request)).scalars():
        child_request = json.loads(payload)
        parent_id = (child_request.get("tags") or {}).get("parent")
        if parent_id:
            historical_intent_accounts.setdefault(parent_id, []).append(child_request.get("account"))
    for row in connection.execute(sa.select(parents).where(parents.c.kind == "routed_limit")).mappings().all():
        snapshot = json.loads(row["payload"])
        request = snapshot["request"]
        venues = {member["market_id"].split(":", 1)[0]
                  for member in request["params"]["bucket"]["members"] if member.get("enabled", True)}
        accounts = {}

        def remember(account):
            if not account or account.get("venue") not in venues:
                return
            venue = account["venue"]
            if venue in accounts and account_key(accounts[venue]) != account_key(account):
                raise ValueError(f"{row['id']}: conflicting historical execution accounts; explicit review required")
            accounts[venue] = account

        remember(snapshot.get("account"))
        children = connection.execute(sa.select(orders.c.payload).where(orders.c.parent_id == row["id"])).scalars()
        for payload in children:
            remember(json.loads(payload).get("account"))
        # Uncertain children may have an intent but no returned order yet.
        for account in historical_intent_accounts.get(row["id"], []):
            remember(account)
        for venue, account in mappings.get(row["id"], {}).items():
            if account.get("venue") != venue or venue not in venues:
                raise ValueError(f"{row['id']}: invalid explicit execution account mapping")
            remember(account)
        if set(accounts) != venues:
            missing = ", ".join(sorted(venues - set(accounts)))
            raise ValueError(f"{row['id']}: historical execution accounts missing for {missing}; "
                             "review and populate meta.legacy_execution_accounts before upgrade")
        accounts = dict(sorted(accounts.items()))
        snapshot["legacy_execution_identity"] = {"venue": snapshot.get("venue"), "account": snapshot.get("account")}
        snapshot.update(venue=None, account=None, execution_accounts=accounts)
        request["account"] = None
        request["params"]["execution_accounts"] = accounts
        connection.execute(parents.update().where(parents.c.id == row["id"]).values(
            account_key=None, payload=json.dumps(snapshot)))
        for venue, account in accounts.items():
            connection.execute(membership.insert().values(parent_id=row["id"], venue=venue, account_key=account_key(account)))
        projections = connection.execute(sa.select(orders).where(
            orders.c.id == row["id"], orders.c.parent_id.is_(None))).mappings().all()
        for projection in projections:
            payload = json.loads(projection["payload"])
            if payload.get("held_by") != "engine":
                continue
            payload.update(venue=None, account=None, execution_accounts=accounts)
            connection.execute(orders.update().where(orders.c.venue == projection["venue"], orders.c.id == row["id"]).values(
                venue=NAMESPACE, account_key=NAMESPACE, payload=json.dumps(payload)))
    connection.execute(meta.update().where(meta.c.key == "schema_version").values(value="11"))


def downgrade():
    raise RuntimeError("execution-scope downgrade requires an explicit compatible backup/restore plan")
