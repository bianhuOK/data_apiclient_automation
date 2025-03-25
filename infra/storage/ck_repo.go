package storage

import (
	"context"
	"database/sql"
)

type CkRepoInterface interface {
	Query(ctx context.Context, sql string, args ...interface{}) (rows []map[string]interface{}, err error)
	Exec(ctx context.Context, sql string, args ...interface{}) (err error)
}

type CkRepo struct {
	Dsn    string
	Client *sql.DB
}

func NewCkRepo(dsn string) (*CkRepo, error) {
	conn, err := sql.Open("chhttp", dsn)
	if err != nil {
		return nil, err
	}
	err = conn.Ping()
	if err != nil {
		return nil, err
	}
	return &CkRepo{
		Dsn:    dsn,
		Client: conn,
	}, nil
}

func (c *CkRepo) Query(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, error) {
	// 执行SQL查询
	rows, err := c.Client.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 获取列名
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// 准备结果集
	var result []map[string]interface{}

	// 遍历结果行
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// 扫描当前行的值
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// 将当前行转换为map
		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			row[col] = val
		}

		result = append(result, row)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return result, nil
}
