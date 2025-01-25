from sqlalchemy import create_engine, text
import os

# Directly assign the connection string
db_connection_string = "mysql+pymysql://sql12759491:Cg2rY4GQam@sql12.freesqldatabase.com:3306/sql12759491?auth_plugin=mysql_native_password"

# Create the SQLAlchemy engine with the correct connection string
engine = create_engine(db_connection_string)


# Function to load all jobs from the database
def load_jobs_from_db():
  try:
    with engine.connect() as conn:
      result = conn.execute(text("SELECT * FROM jobs"))
      jobs = [dict(row) for row in result.all()]
    return jobs
  except Exception as e:
    print(f"Error loading jobs: {e}")


# Function to load a specific job by ID
def load_job_from_db(job_id):
  try:
    with engine.connect() as conn:
      result = conn.execute(text("SELECT * FROM jobs WHERE id = :job_id"),
                            {"job_id": job_id})
      rows = result.all()

      if not rows:
        return None
      return dict(rows[0]._mapping)  # Return the first row as a dictionary
  except Exception as e:
    print(f"Error loading job: {e}")


# Function to add an application to the database
def add_application_to_db(job_id, data):
  try:
    with engine.connect() as conn:
      query = text("""
                INSERT INTO applications 
                (job_id, full_name, email, linkedin_url, education, work_experience, resume_url)
                VALUES 
                (:job_id, :full_name, :email, :linkedin_url, :education, :work_experience, :resume_url)
            """)
      conn.execute(
          query, {
              "job_id": job_id,
              "full_name": data["full_name"],
              "email": data["email"],
              "linkedin_url": data["linkedin_url"],
              "education": data["education"],
              "work_experience": data["work_experience"],
              "resume_url": data["resume_url"]
          })
      print("Application added successfully!")
  except Exception as e:
    print(f"Error while adding application: {e}")


# Test the connection to the database
try:
  with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print("Connection successful:", result.scalar())
except Exception as e:
  print("Error connecting to the database:", e)
