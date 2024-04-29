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
) WITH (
    TTL = Interval("PT72H") ON `created_at`
);

create table `agreement` (
    id Text,
    product Text,
    buid Text,
    `status` Text,
    opening_date Date,
    closing_date Date,
    auth_level Text,
    revision Int64,
    PRIMARY KEY (id)
);

create table `account` (
    `number` Text,
    buid Text,
    agreement_id Text,
    auth_level Text,
    `status` Text,
    opening_date Date,
    closing_date Date,
    PRIMARY KEY (`number`)
);

alter table `account` add index `account_agreement_id_idx` global async on (`agreement_id`);
alter table `account` add index `account_buid_idx` global async on (`buid`);

create table `account_number_sequence` (
    `balance_position` Text,
    current_value Int64,
    PRIMARY KEY (`balance_position`)
);

create table `agreement_audit` (
    revision Int64,
    agreement_id Text,
    product Text,
    buid Text,
    `status` Text,
    opening_date Date,
    closing_date Date,
    auth_level Text,
    PRIMARY KEY (agreement_id, revision)
);

insert into `account_number_sequence`(balance_position, current_value)
    values ('47423', 0), ('40903', 0), ('42301', 0), ('47411', 0), ('47422', 0), ('40914', 0);

create table transaction_event (
    id Text,
    ref_id Text,
    authorization_id Text,
    status Text,
    isoDirection Text,
    isoClass Text,
    isoCategory Text,
    transactionDate Date,
    rrn Text,
    orn Text,
    transaction_amount Text,
    receiver_agreement_id Text,
    originator_agreement_id Text,
    created_at Timestamp,
    PRIMARY KEY (id)
)