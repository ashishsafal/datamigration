# Data Migration Docker Setup

This guide will help you run the Data Migration application in Docker containers.

## Prerequisites

- Docker Desktop installed
- Docker Compose installed

## Quick Start

### Option 1: Using Docker Compose (Recommended)

1. **Build and run all services:**
   ```bash
   docker-compose up --build
   ```

2. **Run in background:**
   ```bash
   docker-compose up -d --build
   ```

3. **Access the application:**
   - Open your browser and go to: `http://localhost:5000`

4. **Stop the services:**
   ```bash
   docker-compose down
   ```

### Option 2: Using Docker only

1. **Build the Docker image:**
   ```bash
   docker build -t datamigration-app .
   ```

2. **Run the container:**
   ```bash
   docker run -d \
     --name datamigration \
     -p 5000:8080 \
     -e ConnectionStrings__SqlServer="Server=your-sql-server;Database=predebug;User Id=qa;Password=admin@123;TrustServerCertificate=True;" \
     -e ConnectionStrings__PostgreSql="Host=your-postgres-host;Database=navikaran_mig;Username=postgres;Password=your-password;Include Error Detail=true;" \
     datamigration-app
   ```

## Configuration

### Database Connections

The application supports two connection configurations:

1. **External Databases** (Default in docker-compose.yml):
   - Uses your existing SQL Server at `192.168.3.250`
   - Uses your existing PostgreSQL RDS instance
   
2. **Containerized Databases** (Optional):
   - SQL Server and PostgreSQL containers are included in docker-compose.yml
   - Update the connection strings in docker-compose.yml environment variables

### Environment Variables

You can override connection strings using environment variables:

```bash
# SQL Server Connection
ConnectionStrings__SqlServer="Server=your-server;Database=your-db;User Id=user;Password=pass;TrustServerCertificate=True;"

# PostgreSQL Connection  
ConnectionStrings__PostgreSql="Host=your-host;Database=your-db;Username=user;Password=pass;Include Error Detail=true;"
```

### Ports

- **Application**: `http://localhost:5000`
- **SQL Server** (if using container): `localhost:1433`
- **PostgreSQL** (if using container): `localhost:5432`

## Development

### Local Development with Docker

```bash
# Build and run for development
docker-compose up --build

# View logs
docker-compose logs -f datamigration

# Restart specific service
docker-compose restart datamigration

# Remove all containers and volumes
docker-compose down -v
```

### Updating the Application

1. Make your code changes
2. Rebuild and restart:
   ```bash
   docker-compose up --build datamigration
   ```

## Troubleshooting

### Common Issues

1. **Connection Refused Errors:**
   - Check if your database servers are accessible from Docker
   - For local databases, use `host.docker.internal` instead of `localhost`

2. **Port Conflicts:**
   - Change the port mapping in docker-compose.yml: `"5001:8080"`

3. **Database Connection Issues:**
   - Verify connection strings in environment variables
   - Check network connectivity between containers

### Useful Commands

```bash
# View running containers
docker ps

# Check application logs
docker logs datamigration-app

# Execute commands inside container
docker exec -it datamigration-app bash

# Remove all stopped containers
docker container prune

# Remove unused images
docker image prune
```

## Security Notes

- Update default passwords in docker-compose.yml before production use
- Consider using Docker secrets for sensitive data
- Ensure database access is properly secured
- Use environment-specific configuration files

## Production Deployment

For production deployment:

1. Update passwords and connection strings
2. Use proper SSL certificates
3. Configure proper logging
4. Set up health checks
5. Use orchestration tools like Kubernetes for scaling

Example production run:
```bash
docker run -d \
  --name datamigration-prod \
  --restart unless-stopped \
  -p 80:8080 \
  -e ASPNETCORE_ENVIRONMENT=Production \
  -e ConnectionStrings__SqlServer="your-production-sql-connection" \
  -e ConnectionStrings__PostgreSql="your-production-postgres-connection" \
  datamigration-app
```