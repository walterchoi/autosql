# Local development

The `tests/docker-init` folder contains a prebuilt Docker Compose setup to run AutoSQL tools
locally. This is especially useful for integration testing or working with supported databases in
a consistent environment.

## Folder structure

```
/tests
  ├── utils/
  │   └── config.local.json   ← Configuration file used by tests and docker
  └── docker-init/
      ├── docker-compose.yml  ← Starts all test containers
      └── .env                ← (Optional) Environment variables for overrides
```

## Running Docker containers

Navigate to the `docker-init` directory and run:

```bash
cd tests/docker-init
docker-compose up
```

This spins up the configured containers (e.g. Postgres, MySQL, SQL Server) defined in
`docker-compose.yml`.

## Configuration matching

Make sure the contents of `config.local.json` in `tests/utils/` match the credentials and ports
defined in `docker-compose.yml`. This ensures AutoSQL tests can connect to the correct database
containers.

For example, if `docker-compose.yml` sets the MySQL container like this:

```yaml
mysql:
  image: mysql:8
  ports:
    - "3307:3306"
  environment:
    MYSQL_USER: testuser
    MYSQL_PASSWORD: testpass
    MYSQL_DATABASE: testdb
```

Then your `config.local.json` should include:

```json
{
  "mysql": {
    "host": "localhost",
    "port": 3307,
    "username": "testuser",
    "password": "testpass",
    "database": "testdb"
  }
}
```

This setup helps avoid mismatched credentials or ports during testing.
