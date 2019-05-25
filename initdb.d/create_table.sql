USE db_olive;

CREATE TABLE `users` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  KEY `id` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `users` (`id`, `name`) VALUES (1, 'pyhons');

CREATE TABLE customers (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  first_name varchar(255),
  last_name varchar(255),
  first_kana varchar(255),
  last_kana varchar(255),
  tel varchar(255),
  foxed_line_tel varchar(255),
  pc_mail varchar(255),
  phone_mail varchar(255),
  can_receive_mail tinyint(1) DEFAULT 1,
  birthday date,
  zip_code varchar(255),
  prefecture varchar(255),
  city text,
  address text,
  comment text,
  first_visit_store_id bigint(20),
  last_visit_store_id bigint(20),
  first_visited_at date,
  last_visited_at date,
  card_number varchar(255),
  introducer varchar(255),
  searchd_by varchar(255),
  has_registration_card tinyint(1),
  children_count int(11),
  created_at datetime NOT NULL,
  updated_at datetime NOT NULL,
  occupation_id bigint(20),
  zoomancy_id bigint(20),
  baby_age_id bigint(20),
  size_id bigint(20),
  visit_reason_id bigint(20),
  nearest_station_id bigint(20),
  email varchar(255) NOT NULL,
  encrypted_password varchar(255),
  reset_password_token varchar(255),
  reset_password_sent_at datetime,
  remember_created_at datetime,
  provider varchar(255) NOT NULL DEFAULT 'email',
  uid varchar(255) NOT NULL,
  tokens text,
  allow_password_change tinyint(1),
  primary key(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;