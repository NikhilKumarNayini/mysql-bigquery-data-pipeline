import pandas as pd
from sqlalchemy import create_engine, text
import pymysql  # or psycopg2 for PostgreSQL

class NthHighestSalary:
    def __init__(self, connection_string):
        """
        Initialize database connection
        connection_string: 'mysql+pymysql://username:password@host:port/database'
        """
        self.engine = create_engine(connection_string)

    def get_nth_highest_by_dept(self, n, department=None):
        """
        Get nth highest salary employees by department

        Args:
            n (int): Rank (1 for highest, 2 for second highest, etc.)
            department (str, optional): Filter by specific department

        Returns:
            pandas.DataFrame: Results with nth highest salary employees
        """
        query = text("""
                     WITH ranked_employees AS (
                         SELECT
                             e.*,
                             d.department_name,
                             DENSE_RANK() OVER (
                                 PARTITION BY d.department_name
                                 ORDER BY e.salary DESC
                                 ) as salary_rank
                         FROM EMPLOYEES e
                                  JOIN Department d ON e.dept = d.dept
                         WHERE (:dept IS NULL OR d.department_name = :dept)
                     )
                     SELECT
                         id,
                         name,
                         Designation,
                         manager,
                         hired_on,
                         salary,
                         commission,
                         dept,
                         department_name,
                         salary_rank
                     FROM ranked_employees
                     WHERE salary_rank = :n
                     ORDER BY department_name, salary DESC
                     """)

        try:
            df = pd.read_sql(
                query,
                self.engine,
                params={"n": n, "dept": department}
            )
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def get_all_departments(self):
        """Get list of all departments"""
        query = text("SELECT department_name FROM Department ORDER BY department_name")
        try:
            df = pd.read_sql(query, self.engine)
            return df['department_name'].tolist()
        except Exception as e:
            print(f"Error fetching departments: {e}")
            return []

# Usage example
def main():
    # Database connection
    connection_string = 'mysql+pymysql://root:password@localhost:3306/company_db'
    salary_tool = NthHighestSalary(connection_string)

    # Get user input
    print("=== Nth Highest Salary by Department ===\n")

    try:
        n = int(input("Enter the rank N (e.g., 1 for highest, 2 for second highest): "))

        # Optional: Filter by specific department
        departments = salary_tool.get_all_departments()
        if departments:
            print(f"\nAvailable departments: {', '.join(departments)}")
            filter_dept = input("Enter department name (or press Enter for all departments): ").strip()
            if filter_dept == "":
                filter_dept = None
        else:
            filter_dept = None

        # Get results
        results = salary_tool.get_nth_highest_by_dept(n, filter_dept)

        if results is not None and not results.empty:
            print(f"\n{'='*60}")
            print(f"Employees with {n}{'st' if n==1 else 'nd' if n==2 else 'rd' if n==3 else 'th'} Highest Salary")
            if filter_dept:
                print(f"Department: {filter_dept}")
            print(f"{'='*60}\n")

            # Format and display results
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 30)

            print(results[['name', 'department_name', 'salary', 'Designation', 'salary_rank']].to_string(index=False))

            # Summary statistics
            print(f"\n📊 Summary:")
            print(f"   Total employees found: {len(results)}")
            print(f"   Average salary: ${results['salary'].mean():,.2f}")
            print(f"   Salary range: ${results['salary'].min():,.2f} - ${results['salary'].max():,.2f}")

            # Group by department
            if not filter_dept:
                print(f"\n📈 By Department:")
                dept_summary = results.groupby('department_name').agg({
                    'name': 'count',
                    'salary': ['mean', 'min', 'max']
                }).round(2)
                dept_summary.columns = ['Count', 'Avg Salary', 'Min Salary', 'Max Salary']
                print(dept_summary.to_string())
        else:
            print(f"\n❌ No employees found with rank {n} {'in ' + filter_dept if filter_dept else ''}")

    except ValueError:
        print("❌ Invalid input. Please enter a valid number.")
    except Exception as e:
        print(f"❌ An error occurred: {e}")

if __name__ == "__main__":
    main()