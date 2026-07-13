CREATE TABLE Departments (
    DepartmentID INTEGER PRIMARY KEY,
    DepartmentName VARCHAR(50),
    Location VARCHAR(50)
);

INSERT INTO Departments
(DepartmentID, DepartmentName, Location)
VALUES
(1,'HR','London'),
(2,'Finance','Frankfurt'),
(3,'IT','Berlin'),
(4,'Sales','Paris'),
(5,'Marketing','Madrid');

CREATE TABLE Employees (
 EmployeeID INTEGER PRIMARY KEY,
 FirstName VARCHAR(50),
 LastName VARCHAR(50),
 Gender VARCHAR(10),
 Age INTEGER,
 DepartmentID INTEGER,
 City VARCHAR(50),
 Salary INTEGER,
 FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
);

INSERT INTO Employees
(EmployeeID, FirstName, LastName, Gender, Age, DepartmentID, City, Salary)
VALUES
(101,'Emma','Wilson','Female',28,1,'London',45000),
(102,'Liam','Smith','Male',35,2,'Manchester',62000),
(103,'Sophia','Brown','Female',31,3,'Berlin',70000),
(104,'Noah','Taylor','Male',42,4,'Paris',68000),
(105,'Olivia','Martin','Female',26,5,'Madrid',48000),
(106,'Lucas','Muller','Male',38,3,'Munich',82000),
(107,'Isabella','Garcia','Female',30,2,'Barcelona',61000),
(108,'Ethan','Johnson','Male',45,1,'Dublin',75000),
(109,'Mia','Anderson','Female',27,4,'Amsterdam',52000),
(110,'James','Thomas','Male',33,3,'London',73000);

-- 1. Display all employees.
select * from employees;

--2. Display only the employee names and salaries.
select firstname, lastname, salary from employees;

--3. Count the total number of employees.
select count(*) from employees;

--4. Display all unique cities.
select DISTINCT(city) from employees

--5. Display all unique department IDs.
select DISTINCT(departmentid) from employees;

--6. Find employees older than 30.
select * from employees where age > 30

--7. Find employees earning more than 60,000.
select * from employees where salary > 60000

--8. Display employees from London.
select * from employees where city = 'London'

--9. Find employees whose salary is between 50,000 and 75,000.
select * from employees where salary between 50000 and 75000

--10. Display employees whose first name starts with L.
select * from employees where firstname like 'L%'

--11. Display employees whose age is less than 35.
select * from employees where age < 35

--12. Sort employees by salary (highest first).
select * from employees order by salary desc

--13. Sort employees by age (youngest first).
select * from employees order by age asc

--14. Sort employees by city and then salary.
select * from employees order by city asc,salary asc

--15. Find the average salary.
select avg(salary) from employees

--16. Find the highest salary.
select max(salary) from employees

--17. Find the minimum salary.
select min(salary) from employees

--18. Find the average employee age.
select avg(age) as mean_age from employees

--19. Count employees in each department.
select departmentid, count(departmentid) from employees group by departmentid order by departmentid

--20. Find the average salary in each department.
select departmentid, avg(salary) as mean_salary from employees group by departmentid order by departmentid

--21. Find the highest salary in each department.
select departmentid, max(salary) as max_salary from employees group by departmentid order by departmentid

--22. Show only departments having more than one employee.
select departmentid, count(departmentid) from employees group by departmentid having count(departmentid) > 1 order by departmentid

--23. Increase salaries of IT employees by 5%.
update employees set salary = salary * 1.05 where departmentid = 3;

--24. Change the city of EmployeeID 109 to Brussels.
update employees set city = 'Brussels' where  employeeid = 109;

--25. Delete employees whose salary is below 48,000.
delete from employees where salary < 48000

--26. Display each employee along with their department name.
select e.*, d.departmentname  from employees e inner join departments d on e.departmentid = d.departmentid

--27. Display employee name, department name, and department location.
select e.firstname, e.lastname, d.departmentname, d.location  from employees e inner join departments d on e.departmentid = d.departmentid


--28. Count the number of employees in each department using a JOIN.
select d.departmentname, count(e.*) from employees e inner join departments d on e.departmentid = d.departmentid group by d.departmentname

--29. Display all departments, even if they have no employees.
select d.*, e.*  from employees e right join departments d on e.departmentid = d.departmentid

--30. Find the average salary for each department using a JOIN.
select d.departmentname, avg(e.salary) from employees e inner join departments d on e.departmentid = d.departmentid group by d.departmentname

--31. Display employees who work in departments located in Berlin.
select d.location, e.* from employees e inner join departments d on e.departmentid = d.departmentid where d.location = 'Berlin'

--32. Display only employees working in the IT department (using a JOIN instead of filtering by ID).
select d.departmentname, e.* from employees e inner join departments d on e.departmentid = d.departmentid where d.departmentname = 'IT'

