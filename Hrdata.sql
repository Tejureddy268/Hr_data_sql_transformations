# creating table for employees

create table employees (
id INT PRIMARY KEY, 
name VARCHAR(50),
department VARCHAR(50), 
salary DECIMAL (10,2),
joining_date DATE
);


DROP TABLE employees;

# creating table for employees
create table employees (
id INT PRIMARY KEY, 
name VARCHAR(50),
department VARCHAR(50), 
salary DECIMAL (10,2),
joining_date DATE
);

# Inserting data values into employees table

insert into employees (id , name , department, salary, joining_date) values
(1, 'Alice', 'HR', 60000, '2022-01-15'),
(2, 'Bob', 'Finance', 75000, '2021-11-20'),
(3, 'charlie', 'IT', 80000, '2020-06-10'),
(4, 'David', 'IT', 85000, '2019-05-25'),
(5, 'Eve' , 'Hr', 62000, '2023-03-05'); 

# Transforming data in the employees table using various SQL queries

# selecting all employees

select* from employees;

#flitering employees from IT department

select * from employees where department = 'IT';

# employees with salary greater than 70000

select * from employees where salary > 70000

# sort employees by salary Descending order

select * from employees ORDER BY salary DESC;

#finding the total salary paid

select sum(salary
)as total_salary from employees;

# counting number of employees

select count(*) as employee_count from employees;

#finding average salary per department

select department, avg(salary) as avg_salary
from employees
group by department;


#joining two tables

create table departments (
department_id INT PRIMARY KEY,
department_name VARCHAR(50)
);

#inserting data into departments table

insert into departments (department_id, department_name)values
(1, 'HR'),
(2, 'Finance'),
(3, 'IT');

#joining two tables

select e.name, d.department_name
from employees e
join departments d on e.department = d.department_name;


# finding employees who earns more than the average salary

select * from employees
where salary > (select avg(salary) from employees);


# finding the highest salary

#finding oldest employee by joining date
select * from employees order by joining_date asc limit 1;

#finding the highest salary in each department
#using window functions
select name, department, salary, RANK() over (partition by department order by salary desc) as "RANK"
from employees;


#Recursive query to find employee hierarchy
create table managers (
id INT PRIMARY KEY,
name VARCHAR(50),
manager_id int
);


INSERT into managers (id, name , manager_id) values
(1, 'Alice', NULL),
(2, 'Bob', 1),
(3, 'charlie', 2),
(4, 'david', 3);

#Recursive query to find employee hierarchy

with recursive employee_hierarchy as (
select id, name, manager_id from managers where manager_id is null 
union all
select m.id, m.name, m.manager_id
from managers m
join employee_hierarchy eh on m.manager_id = eh.id
)

# selecting the employee hierarchy
select * from employee_hierarchy;

# finding employees with same name

select name, count(*) as count
from employees
group by name
having count(*) > 1;

# finding the department with the highest average salary

select department 
from (select department, avg(salary) as avg_salary
from employees
group by department) as avg_salaries
order by avg_salary desc
limit 1;


#finding employees who joined in the last 6 months

SELECT * 
FROM employees
WHERE joining_date <= DATE_SUB(CURDATE(), INTERVAL 6 MONTH);
