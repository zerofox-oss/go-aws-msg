CREATE DATABASE tests
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

CREATE USER name WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE "tests" to name;
\c tests;
GRANT pg_read_all_data TO name;
GRANT pg_write_all_data TO name;
create table client(l int, md5 char(32), msg text);
create index client_l_idx on client(l);
create index client_md5_idx on client(md5);

-- create table server(l int, md5 char(32), msg char(80)); - fails on unicode with: cannot find encode plan
create table server(l int, md5 char(32), msg text);
create index server_l_idx on server(l);
create index server_md5_idx on server(md5);
create table too_long(olen int, original text, elen int, encoded text);
create table sent(id char(20), msg text, overflow int);
create index sent_id_idx on sent(id);




