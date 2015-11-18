create table Plan(
	name varChar(50),
	plid int primary key,
	cost float,
	movieLimit int);

create table Address(
	aid int primary key,
	streetAddress varChar(100),
	city varChar(30),
	state varChar(30));

create table Customers(
	cid int primary key,
	name varChar(50),
	aid int references address(aid),
	password varChar(20),
	username Varchar(30), 
	plid int references plan(plid));

create table Movies(
	mid int primary key, 
	name varChar(100),
	cid int references customers(cid));

create table Rents(
	cid int references customers(cid),
	mid int references movies(mid),
	dateStart date,
	dateEnd date);


insert into plan VALUES('Basic', 1, 29.99, 5);
insert into plan VALUES('Basic Plus', 2, 39.99, 8);
insert into plan VALUES('Unlimited', 3, 69.99, 2147483647);

insert into address values (1,'6 Trudeau Drive','Sandford','Vermont');
insert into customers VALUES (1, 'Alice Bobson', 1, 'abc123', 'abobson', 2);

insert into address values (2,'1 Infinite Loop','Cupertino','California');
insert into customers values (2, 'Timothy Cook', 1, 'microsoftsucks', '2manycooks', 3);

insert into movies (mid, name) values (547385,'Toy Story');
insert into movies (mid, name) values (1,'Terminator: Salvation');
insert into movies (mid, name) values (2,'Placeholder 1');
insert into movies (mid, name) values (3,'Placeholder 2');
insert into movies (mid, name) values (4,'Placeholder 3');
insert into movies (mid, name) values (5,'Placeholder 4');

insert into rents (cid, mid, dateStart) values (1, 547385, '2015-01-01');
update movies set cid = 1 where mid = 547385;

insert into rents (cid, mid, dateStart) values (1, 1, '2015-01-03');
update movies set cid = 1 where mid = 1;

insert into rents (cid, mid, dateStart) values (1, 2, '2015-01-03');
update movies set cid = 1 where mid = 2;

insert into rents (cid, mid, dateStart) values (1, 3, '2015-01-03');
update movies set cid = 1 where mid = 3;

insert into rents (cid, mid, dateStart) values (1, 4, '2015-01-03');
update movies set cid = 1 where mid = 4;

insert into rents (cid, mid, dateStart) values (1, 5, '2015-01-03');
update movies set cid = 1 where mid = 5;
