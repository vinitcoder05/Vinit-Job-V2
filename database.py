from sqlalchemy import create_engine, text
import os

# Database connection setup
db_connection_string = os.environ['DB_CONNECTION_STRING']

engine = create_engine(db_connection_string,
                       connect_args={"ssl": {
                           "ssl_ca": "/etc/ssl/cert.pem"
                       }})


# Load all jobs from the database
def load_jobs_from_db():
  with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM jobs"))
    jobs = [dict(row) for row in result.all()]
  return jobs


# Load a specific job by ID
def load_job_from_db(job_id):
  with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM jobs WHERE id = :job_id"),
                          {"job_id": job_id})
    rows = result.all()

    if not rows:
      return None
    return dict(rows[0]._mapping)  # Return the first row as a dictionary


# Add a new application to the database
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
