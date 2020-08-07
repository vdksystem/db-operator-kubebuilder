package postgres

import (
	"database/sql"
	clarizencloudv1beta1 "db-operator/api/v1beta1"
	"db-operator/controllers/user"
	"fmt"
	_ "github.com/lib/pq"
	"os"
)

type Postgres struct {
	conn *sql.DB
	dbs  *clarizencloudv1beta1.Database
	Role string
}

func NewDB(database *clarizencloudv1beta1.Database) (*Postgres, error) {
	var err error
	pg := &Postgres{}
	pg.Role = fmt.Sprintf("%s_admins", database.Name)
	pg.dbs = database
	connStr := fmt.Sprintf("dbname=%s user=%s password=%s host=%s sslmode=disable",
		os.Getenv("PGDATABASE"),
		os.Getenv("PGUSER"),
		os.Getenv("PGPASSWORD"),
		os.Getenv("PGHOST"))
	pg.conn, err = sql.Open("postgres", connStr)
	if err != nil {
		return pg, err
	}

	if err = pg.conn.Ping(); err != nil {
		return pg, err
	}

	return pg, nil
}

func (pg *Postgres) Exists() (bool, error) {
	var exists bool
	query := fmt.Sprintf("SELECT EXISTS(SELECT datname FROM pg_catalog.pg_database WHERE datname = '%s');", pg.dbs.Name)
	row := pg.conn.QueryRow(query)
	err := row.Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (pg *Postgres) CreateDatabase() error {
	query := fmt.Sprintf(`CREATE DATABASE "%s"`, pg.dbs.Name)
	_, err := pg.conn.Exec(query)
	if err != nil {
		return err
	}

	if pg.dbs.Spec.Schema != "" {
		query := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS "%s"`, pg.dbs.Name)
		_, err := pg.conn.Exec(query)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pg *Postgres) CreateUser(user *user.User) error {
	query := fmt.Sprintf(`CREATE ROLE "%s"`, pg.Role)
	_, err := pg.conn.Exec(query)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`GRANT ALL on DATABASE "%s" to "%s"`, user.Username, pg.Role)
	_, err = pg.conn.Exec(query)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`CREATE USER "%s" WITH ENCRYPTED PASSWORD '%s'`, user.Username, user.Password)
	_, err = pg.conn.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func (pg *Postgres) RoleUsers() ([]string, error) {
	queryString := `select usename
		from pg_user
		join pg_auth_members on (pg_user.usesysid = pg_auth_members.member)
		join pg_roles on (pg_roles.rolname = '%s' AND pg_roles.oid = pg_auth_members.roleid)`

	query := fmt.Sprintf(queryString, pg.Role)

	rows, err := pg.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []string

	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, err
		}
		users = append(users, role)
	}

	return users, nil
}

func (pg *Postgres) Grant(user string) error {
	query := fmt.Sprintf(`GRANT "%s" to "%s"`, pg.Role, user)
	_, err := pg.conn.Exec(query)
	if err != nil {
		// TODO: handle no such role error
		return err
	}

	return nil
}

func (pg *Postgres) Revoke(user string) error {
	query := fmt.Sprintf(`REVOKE "%s" FROM "%s"`, pg.Role, user)
	_, err := pg.conn.Exec(query)
	if err != nil {
		// TODO: handle no such role error
		return err
	}
	return nil
}

func (pg *Postgres) DropDatabase() error {
	query := fmt.Sprintf(`DROP DATABASE "%s"`, pg.dbs.Name)
	_, err := pg.conn.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func (pg *Postgres) DropUser(user string) error {
	query := fmt.Sprintf(`DROP USER "%s"`, user)
	_, err := pg.conn.Exec(query)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`REVOKE ALL ON DATABASE "%s" FROM "%s"`, pg.dbs.Name, pg.Role)
	_, err = pg.conn.Exec(query)
	if err != nil {
		return err
	}

	query = fmt.Sprintf(`DROP ROLE "%s"`, pg.Role)
	_, err = pg.conn.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func (pg *Postgres) GetHost() string {
	return os.Getenv("PGHOST")
}
