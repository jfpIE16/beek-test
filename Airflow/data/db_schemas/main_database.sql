USE beek_test;

CREATE TABLE users (
	id INT NOT NULL AUTO_INCREMENT,
	first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    user_name VARCHAR(50) NOT NULL,
    created_at DATETIME NOT NULL,
	PRIMARY KEY (id)
);

CREATE TABLE audiobook_subscriptions (
    id INT NOT NULL AUTO_INCREMENT,
    user_id INT NOT NULL,
    payment_id INT NOT NULL,
    status INT,
    cancellation_intent VARCHAR(50),
    subscription_started_at DATETIME NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE payment (
    id INT NOT NULL AUTO_INCREMENT,
    payment_status VARCHAR(50),
    payment_date DATETIME,
    next_payment_attempt DATETIME,
    PRIMARY KEY (id)
);

INSERT INTO beek_test.users
(first_name, last_name, user_name, created_at)
VALUES('Jose Fernando', 'Perez', 'jf23perez', '2021-10-10 13:56:45');

INSERT INTO beek_test.payment
(payment_status, payment_date, next_payment_attempt)
VALUES('Accepted', '2021-11-24 13:56:45', '2021-12-24 13:56:45');

INSERT INTO beek_test.audiobook_subscriptions
(user_id, payment_id, status, cancellation_intent, subscription_started_at)
VALUES(1, 1, 1, 'Nothing', '2021-11-24 13:56:45');




