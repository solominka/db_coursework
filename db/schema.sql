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

create table `account` (
    `number` Text,
    agreement_id Text,
    `status` Text,
    opening_date Date,
    closing_date Date,
    PRIMARY KEY (`number`)
);

create table `account_number_sequence` (
    `balance_position` Text,
    current_value Int64,
    PRIMARY KEY (`balance_position`)
);

insert into `account_number_sequence`(balance_position, current_value)
    values (47423, 0), (40903, 0), (42301, 0), (47411, 0), (47422, 0), (47423, 0);