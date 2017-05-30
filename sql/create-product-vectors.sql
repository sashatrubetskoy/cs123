-- Nick McDonnell, Vinay Reddy, Sasha Trubetskoy
-- CS 123 Final Project
--
-- Creates the database that our algorithms use.

CREATE TABLE titles
(
	product_id varchar(12), 
	title varchar(300)
);

.separator "|"
.import id_product.txt titles


CREATE TABLE vectors
	(product_id varchar(12),
		vector varchar(4000));

.separator "\t"
.import filtered_clothes_vectors.txt vectors


CREATE TABLE vectors_normed
	(product_id varchar(12),
		vector_normed varchar(4000));

.separator "\t"
.import normalized_filtered_clothes_vectors.txt vectors_normed

update vectors set product_id=replace(product_id, '"','');
update vectors_normed set product_id=replace(product_id, '"','');

create table products as select * from titles join vectors on titles.product_id = vectors.product_id join vectors_normed on vectors.product_id = vectors_normed.product_id;

-- We could not get the two lines below to work:
-- alter table products drop column "product_id:1";
-- alter table products drop column "product_id:2";
drop table titles;
drop table vectors;
drop table vectors_normed;
