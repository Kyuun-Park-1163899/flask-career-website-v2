from sqlalchemy import create_engine, text
import os

db_connection_strig = os.environ['DB_CONNECTION_STRING']

engine = create_engine(db_connection_strig)


def load_jobs_from_db():
  with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM jobs"))
    jobs = []
    for row in result.all():
      jobs.append(row._asdict())
    return jobs


def load_job_from_db(id):
  with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM jobs WHERE id = :val"),
                          {"val": id})
    rows = result.all()
    if len(rows) == 0:
      return None
    else:
      return rows[0]._asdict()


def add_application_to_db(job_id, data):
  query = text("""
      INSERT INTO applications (
          job_id, full_name, email, linkedin, education, work_experience, resume
      ) VALUES (
          :job_id, :full_name, :email, :linkedin, :education, :work_experience, :resume
      )
  """)

  with engine.begin() as conn:
    conn.execute(
        query, {
            "job_id": job_id,
            "full_name": data.get('full_name'),
            "email": data.get('email'),
            "linkedin": data.get('linkedin'),
            "education": data.get('education'),
            "work_experience": data.get('work_experience'),
            "resume": data.get('resume')
        })
