-- connect with user root on none (database) 
-- mysql -u root
DROP database if exists dbmovie ;
drop user if exists movie cascade; 

create database dbmovie CHARACTER SET utf8;
use dbmovie;
create user movie@localhost identified by 'password';
grant all privileges on dbmovie.* to 'movie'@'localhost';
flush privileges;
-- you can reconnect now with user movie : 
-- 		mysql -u movie -p dbmovie
--
-- create structure and data :
source my_cine_ddl.sql
set autocommit = 1;
source cine_data_stars.sql
source cine_data_movies.sql
-- source cine_data_play.sql