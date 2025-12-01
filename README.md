# Pro Analytics 02 Python Starter Repository

> Use this repo to start a professional Python project.

- Additional information: <https://github.com/denisecase/pro-analytics-02>
- Project organization: [STRUCTURE](./STRUCTURE.md)
- Build professional skills:
  - **Environment Management**: Every project in isolation
  - **Code Quality**: Automated checks for fewer bugs
  - **Documentation**: Use modern project documentation tools
  - **Testing**: Prove your code works
  - **Version Control**: Collaborate professionally

---

## WORKFLOW 1. Set Up Your Machine

Proper setup is critical.


## WORKFLOW 2. Set Up Your Project

After verifying your machine is set up, set up a new Python project by copying this template. Complete each step in the following guide.

It includes the critical commands to set up your local environment

python -m venv .venv
.venv\Scripts\activate
uv run python -m analytics_project.data_prep
git add .
git commit -m "Add data_prep.py and successfully load raw data into pandas DataFrames"
git push

Issues Encountered
Filename mismatch (customers-data.csv vs customers_data.csv) resolved with Git add-commit-push

## WORKFLOW 3. Daily Workflow

Please ensure that the prior steps have been verified before continuing.
When working on a project, we open just that project in VS Code.

### 3.1 Git Pull from GitHub

Always start with `git pull` to check for any changes made to the GitHub repo.

```shell
git pull
```

### 3.2 Run Checks as You Work

This mirrors real work where we typically:

1. Update dependencies (for security and compatibility).
2. Clean unused cached packages to free space.
3. Use `git add .` to stage all changes.
4. Run ruff and fix minor issues.
5. Update pre-commit periodically.
6. Run pre-commit quality checks on all code files (**twice if needed**, the first pass may fix things).
7. Run tests.

In VS Code, open your repository, then open a terminal (Terminal / New Terminal) and run the following commands one at a time to check the code.

```shell
uv sync --extra dev --extra docs --upgrade
uv cache clean
git add .
uvx ruff check --fix
uvx pre-commit autoupdate
uv run pre-commit run --all-files
git add .
uv run pytest
```

NOTE: The second `git add .` ensures any automatic fixes made by Ruff or pre-commit are included before testing or committing.

<details>
<summary>Click to see a note on best practices</summary>

`uvx` runs the latest version of a tool in an isolated cache, outside the virtual environment.
This keeps the project light and simple, but behavior can change when the tool updates.
For fully reproducible results, or when you need to use the local `.venv`, use `uv run` instead.

</details>

### 3.3 Build Project Documentation

Make sure you have current doc dependencies, then build your docs, fix any errors, and serve them locally to test.

```shell
uv run mkdocs build --strict
uv run mkdocs serve
```

- After running the serve command, the local URL of the docs will be provided. To open the site, press **CTRL and click** the provided link (at the same time) to view the documentation. On a Mac, use **CMD and click**.
- Press **CTRL c** (at the same time) to stop the hosting process.

### 3.4 Execute

This project includes demo code.
Run the demo Python modules to confirm everything is working.

In VS Code terminal, run:

```shell
uv run python -m analytics_project.demo_module_basics
uv run python -m analytics_project.demo_module_languages
uv run python -m analytics_project.demo_module_stats
uv run python -m analytics_project.demo_module_viz
```

You should see:

- Log messages in the terminal
- Greetings in several languages
- Simple statistics
- A chart window open (close the chart window to continue).

If this works, your project is ready! If not, check:

- Are you in the right folder? (All terminal commands are to be run from the root project folder.)
- Did you run the full `uv sync --extra dev --extra docs --upgrade` command?
- Are there any error messages? (ask for help with the exact error)

---

### 3.5 Git add-commit-push to GitHub

Anytime we make working changes to code is a good time to git add-commit-push to GitHub.

1. Stage your changes with git add.
2. Commit your changes with a useful message in quotes.
3. Push your work to GitHub.

```shell
git add .
git commit -m "describe your change in quotes"
git push -u origin main
```

This will trigger the GitHub Actions workflow and publish your documentation via GitHub Pages.

### 3.6 Modify and Debug

With a working version safe in GitHub, start making changes to the code.

Before starting a new session, remember to do a `git pull` and keep your tools updated.

Each time forward progress is made, remember to git add-commit-push.

---

# 🏗️ Data Warehouse – Design, Schema, and ETL Workflow

This section documents the creation of the Smart Store Data Warehouse built for P4.
It includes the star schema design, SQL structures, ETL pipeline, and verification steps.

## ⭐ 1. Data Warehouse Objectives

- Provide a structured star schema for analytics
- Centralize the cleaned datasets from `data/prepared/`
- Improve query performance and simplify BI workflows
- Support future dashboards and KPI calculations

---

## 🧠 2. Warehouse Schema (Star Schema)

The warehouse contains one fact table and two dimension tables.

### 🟩 Dimension Tables
**customer**
- customer_id (PK)
- name
- region
- join_date
- number_of_purchases
- shopping_frequency

**product**
 - product_id (PK),
 - product_name
 - category
 - unit_price
 - stock_quantity
 - supplier
### Fact Table
**sale**
- sale_id (PK)
- customer_id (FK → customer.customer_id)
- product_id (FK → product.product_id)
- sale_amount
- sale_date
- shipping
- state

---

## 📐 3. SQL Schema Used in the Warehouse

```sql
CREATE TABLE IF NOT EXISTS customer (
   customer_id INTEGER PRIMARY KEY,
   name TEXT,
   region TEXT,
   join_date TEXT,
   number_of_purchases INTEGER,
   shopping_frequency TEXT
);

CREATE TABLE IF NOT EXISTS product (
   product_id INTEGER PRIMARY KEY,
   product_name TEXT,
   category TEXT,
   unit_price REAL,
   stock_quantity INTEGER,
   supplier TEXT
);

CREATE TABLE IF NOT EXISTS sale (
   sale_id INTEGER PRIMARY KEY,
   customer_id INTEGER,
   product_id INTEGER,
   sale_amount REAL,
   sale_date TEXT,
   shipping REAL,
   state TEXT,
   FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
   FOREIGN KEY (product_id) REFERENCES product(product_id)
);
```
## 4. Reporting in Power BI

Below is the summary of the slicing, dicing, and drilldown decisions used in the Power BI report.

### Slicing
**Field used:** `customer.join_date`
**Rationale:**
The `sale_date` column contained only one unique date, which made it ineffective as a slicer. Using `join_date` enabled meaningful time-based filtering and allowed users to adjust the timeline of the report.

### Dicing
**Fields used:**
- `product.category`
- `product.supplier`
- `product.stock_quantity`

**Rationale:**
These product attributes allowed for effective data segmentation. A matrix using these fields provided clarity on inventory levels across categories and suppliers, helping identify distribution patterns.

### Drilldown
**Field used:** `customer.join_date`
**Rationale:**
`join_date` supports a full hierarchy (Year → Quarter → Month), enabling users to drill deeper into customer trends over time. This created a more interactive and insightful analysis experience.

## 5. OLAP cubes and dimensions

### OLAP Folder Description
The `olap/` folder contains a single module: `cube.py`. This file:
- Loads the transaction dataset.
- Groups records by `ProductID`.
- Aggregates metrics such as total sales.
- Produces a precomputed cube that can be exported as CSV for downstream analysis.


### **Process**
1. Used Python (Pandas + custom OLAP script) to build a cube aggregated by Product ID.
2. Exported cube as CSV.
3. Imported cube into Power BI and created relationships with product and region tables.
4. Designed multiple visuals including slicers, hierarchy drilldowns, and bar charts.


### **Results**
- A functioning OLAP-like dataset generated in Python.
- A Power BI report enabling:
- Filtering by **Region**, **Unit Price**, and **Stock Levels**.
- Hierarchical drilldown from **Category → Product Name**.
- Visual exploration of product performance.


### Power BI Dashboard
![Dashboard Screenshot](images/image.png)
