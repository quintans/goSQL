create user gosql
      identified by gosql
      default tablespace users
      temporary tablespace temp
      quota unlimited on users;

 grant create session, create table to gosql;
 grant create view, create procedure, create sequence to gosql;