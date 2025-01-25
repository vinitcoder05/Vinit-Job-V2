from sqlalchemy import create_engine, text
import os
import logging

# Enable SQLAlchemy logging for debugging
logging.basicConfig()
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

# Database connection setup
db_connection_string = os.environ['DB_CONNECTION_STRING']

# SSL Configuration: Adjust based on your database server requirements
use_ssl = True
ssl_args = {"ssl": {"ssl_ca": "/etc/ssl/cert.pem"}} if use_ssl else {}

# Create SQLAlchemy engine
engine = create_engine(db_connection_string, connect_args=ssl_args)


# Load all jobs from the database
def load_jobs_from_db():
  try:
    with engine.connect() as conn:
      result = conn.execute(text("SELECT * FROM jobs"))
      jobs = [dict(row) for row in result.all()]
    return jobs
  except Exception as e:
    logging.error(f"Error loading jobs: {e}")
    return []


# Load a specific job by ID
def load_job_from_db(job_id):
  try:
    with engine.connect() as conn:
      result = conn.execute(text("SELECT * FROM jobs WHERE id = :job_id"),
                            {"job_id": job_id})
      rows = result.all()
      if not rows:
        logging.warning(f"No job found with ID: {job_id}")
        return None
      return dict(rows[0]._mapping)  # Return the first row as a dictionary
  except Exception as e:
    logging.error(f"Error loading job with ID {job_id}: {e}")
    return None


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
    logging.error(f"Error adding application for job ID {job_id}: {e}")


# Test database connection for troubleshooting
def test_db_connection():
  try:
    with engine.connect() as conn:
      result = conn.execute(text("SELECT 1"))
      if result.scalar() == 1:
        print("Database connection successful!")
      else:
        print("Database connection failed!")
  except Exception as e:
    logging.error(f"Error testing database connection: {e}")


# Example usage
if __name__ == "__main__":
  # Test database connection
  test_db_connection()

  # Example data
  sample_job_id = 1
  sample_data = {
      "full_name": "John Doe",
      "email": "john.doe@example.com",
      "linkedin_url": "https://linkedin.com/in/johndoe",
      "education": "BSc Computer Science",
      "work_experience": "3 years",
      "resume_url": "http://example.com/resume.pdf"
  }

  # Fetch all jobs
  print(load_jobs_from_db())

  # Fetch a job by ID
  print(load_job_from_db(sample_job_id))

  # Add a new application
  add_application_to_db(sample_job_id, sample_data)
