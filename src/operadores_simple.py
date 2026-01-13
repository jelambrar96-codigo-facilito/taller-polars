import polars as pl


print("hola polars")

# Operadores del algebra relacional
# 1. Seleccion de columnas
# 2. Proyeccion de columnas
# 3. joins
# 4. Unions de columnas


# Employee dataset 
employees = pl.DataFrame(
    {
        "emp_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "dept_id": [1, 2, 1, 3, 2],
        "salary": [60000, 70000, 65000, 75000, 68000], 
    }
)

# Department dataset 
departments = pl.DataFrame(
    {  
        "dept_id": [1, 2, 3], 
        "dept_name": ["Engineering", "Marketing", "Sales"], 
        "location": ["NYC", "SF", "LA"],
    }
)

## projection (pi)

employees
departments


employees.select(["name", "salary"])
departments.select(["dept_id", "dept_name", "location"])


# projection and renaming
departments.select("dept_name")
departments.select(pl.col("dept_name"))
departments.select(pl.col("dept_name").alias("deparment_name"))


## selection (sigma) 

employees.filter(pl.col("salary") > 65000) # you don't need repeat dataframe name

employees.filter(pl.col("salary") > 65000, pl.col("dept_id") == 2) # comma works like an "and" (&)
employees.filter((pl.col("salary") > 65000) | (pl.col("dept_id") == 2)) 

## union (mu)

new_employees = pl.DataFrame({
    "emp_id": [6, 7, 8],
    "name": ["Frank", "Grace", "Henry"],
    "dept_id": [4, 4, 4],
    "salary": [80000, 72000, 62000],
})
employees = pl.concat([employees, new_employees], how="vertical")

## joins ()

# cross does not work, I'm using outer
employees.join(departments, left_on="dept_id", right_on="dept_id", how="outer")


## intersecton (cup)

employees.join(departments, left_on="dept_id", right_on="dept_id", how="inner")

## difference (-)

employees.join(departments, left_on="dept_id", right_on="dept_id", how="anti")

## Division


# First, create a set of required projects 
required_projects = pl.DataFrame({"project_id": ["P1", "P2"]})

# Count how many required projects each employee works on 
emp_project_counts = employees.filter(
    pl.col("project_id").is_in(required_projects["project_id"].to_list())
).with_columns(pl.col("project_id").count().alias("project_count"))

# Find employees who work on all required projects (count matches required p 
employees_all_projects = emp_project_counts.filter(
    pl.col("project_count") == len(required_projects["project_id"].to_list())
)

# Get the final result with employee details 
result = employees.join(employees_all_projects, on="emp_id", how="inner").select(["name", "emp_id"])


