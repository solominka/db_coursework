create table `client` (
    buid Text,
    version Int64,
    `name` Text,
    surname Text,
    patronymic Text,
    phone Text,
    auth_level Text,
    PRIMARY KEY (buid)
);

create table `request` (
    idempotency_token Text,
    body Text,
    request_type Text,
    created_at Timestamp,
    created_entity_id Text,
    PRIMARY KEY (idempotency_token)
);

create table `agreement` (
    id Text,
    buid Text,
    `status` Text,
    opening_date Date,
    closing_date Date,
    auth_level Text,
    PRIMARY KEY (id)
);
