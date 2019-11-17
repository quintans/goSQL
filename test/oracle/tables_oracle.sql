CREATE TABLE "PUBLISHER" (
	"ID" INTEGER NOT NULL,
	"VERSION" INTEGER NOT NULL,
	"NAME" VARCHAR2(50),
	"ADDRESS" VARCHAR2(255),
	PRIMARY KEY(ID)
);

CREATE TABLE "BOOK" (
	"ID" INTEGER NOT NULL,
	"VERSION" INTEGER NOT NULL,
	"NAME" VARCHAR2(100),
	"PUBLISHED" TIMESTAMP,
	"PRICE" NUMBER(18,4),
	"PUBLISHER_ID" INTEGER,
	PRIMARY KEY(ID)
);

CREATE TABLE "BOOK_I18N" (
	"ID" INTEGER NOT NULL,
	"VERSION" INTEGER NOT NULL,
	"BOOK_ID" INTEGER NOT NULL,
	"LANG" VARCHAR2(10),
	"TITLE" VARCHAR2(100),
	PRIMARY KEY(ID)
);

CREATE TABLE "BOOK_BIN" (
	"ID" INTEGER NOT NULL,
	"VERSION" INTEGER NOT NULL,
	"HARDCOVER" BLOB NOT NULL,
	PRIMARY KEY(ID)
);

CREATE TABLE "AUTHOR" (
	"ID" INTEGER NOT NULL,
	"VERSION" INTEGER NOT NULL,
	"NAME" VARCHAR2(50),
	"SECRET" VARCHAR2(50),
	PRIMARY KEY(ID)
);

CREATE TABLE "AUTHOR_BOOK" (
	"AUTHOR_ID" INTEGER NOT NULL,
	"BOOK_ID" INTEGER NOT NULL,
	PRIMARY KEY(AUTHOR_ID, BOOK_ID)
);

ALTER TABLE "BOOK" ADD CONSTRAINT FK_BOOK1 FOREIGN KEY ("PUBLISHER_ID") REFERENCES "PUBLISHER" ("ID");
ALTER TABLE "AUTHOR_BOOK" ADD CONSTRAINT FK_AUTHOR_BOOK1 FOREIGN KEY ("AUTHOR_ID") REFERENCES "AUTHOR" ("ID");
ALTER TABLE "AUTHOR_BOOK" ADD CONSTRAINT FK_AUTHOR_BOOK2 FOREIGN KEY ("BOOK_ID") REFERENCES "BOOK" ("ID");
ALTER TABLE "BOOK_BIN" ADD CONSTRAINT FK_BOOK_BIN1 FOREIGN KEY ("ID") REFERENCES "BOOK" ("ID");
ALTER TABLE "BOOK_I18N" ADD CONSTRAINT FK_BOOK_I18N1 FOREIGN KEY ("BOOK_ID") REFERENCES "BOOK" ("ID");
ALTER TABLE "BOOK_I18N" ADD CONSTRAINT UK_BOOK_I18N1 UNIQUE ("BOOK_ID", "LANG");

CREATE TABLE "PROJECT" (
	"ID" INTEGER NOT NULL,
	"VERSION" INTEGER NOT NULL,
	"NAME" VARCHAR2(50),
	"MANAGER_ID" INTEGER NOT NULL,
	"MANAGER_TYPE" CHAR(1) NOT NULL,
	"STATUS_COD" VARCHAR2(50),
	PRIMARY KEY(ID)
);

CREATE TABLE "CONSULTANT" (
	"ID" INTEGER NOT NULL,
	"VERSION" INTEGER NOT NULL,
	"NAME" VARCHAR2(50),
	PRIMARY KEY(ID)
);

CREATE TABLE "EMPLOYEE" (
	"ID" INTEGER NOT NULL,
	"VERSION" INTEGER NOT NULL,
	"FIRST_NAME" VARCHAR2(50),
	"LAST_NAME" VARCHAR2(50),
	PRIMARY KEY(ID)
);

CREATE TABLE "CATALOG" (
	"ID" INTEGER NOT NULL,
	"VERSION" INTEGER NOT NULL,
	"DOMAIN" VARCHAR2(10),
	"KEY" VARCHAR2(50),
	"VALUE" VARCHAR2(500),
	PRIMARY KEY(ID)
);

CREATE SEQUENCE PUBLISHER_SEQ START WITH 100;
CREATE SEQUENCE BOOK_SEQ START WITH 100;
CREATE SEQUENCE BOOK_I18N_SEQ START WITH 100;
CREATE SEQUENCE BOOK_BIN_SEQ START WITH 100;
CREATE SEQUENCE AUTHOR_SEQ START WITH 100;
CREATE SEQUENCE PROJECT_SEQ START WITH 100;
CREATE SEQUENCE CONSULTANT_SEQ START WITH 100;
CREATE SEQUENCE EMPLOYEE_SEQ START WITH 100;
CREATE SEQUENCE CATALOG_SEQ START WITH 100;

