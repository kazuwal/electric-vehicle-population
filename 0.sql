create user kazuwal with superuser password 'password';

create role dev; -- create role

grant dev to kazuwal; -- grant role

set role kazuwal; -- set current_user

set session authorization 'kazuwal'; -- set session_user

select current_user, session_user;

create database pyspark_sql_recipes;

grant all privileges on database pyspark_sql_recipes to dev;

\connect pyspark_sql_recipes kazuwal;

create schema raw;
