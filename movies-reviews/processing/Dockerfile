FROM python:3.12

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app
COPY . .

RUN poetry export -f requirements.txt --output requirements.txt
RUN pip install -r requirements.txt

CMD ["python3", "-m", "processing"]
