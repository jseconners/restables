FROM python:3.7

LABEL maintainer="jseconners@gmail.com"

WORKDIR /tdvr-python
COPY poetry.lock pyproject.toml ./

RUN pip install poetry
RUN poetry config virtualenvs.create false
RUN poetry install --no-dev

COPY app.py db.py utils.py ./
COPY instance ./instance

# add and set user
RUN groupadd -r tdvr && useradd --no-log-init -r -g tdvr tdvr
RUN chown -R tdvr:tdvr ./
USER tdvr

ENV FLASK_APP app.py

EXPOSE 5000
ENTRYPOINT ["flask", "run", "--host=0.0.0.0"]