package bunwrapper

import (
	"context"
	"errors"

	"github.com/uptrace/bun"
)

type bunWrapper[R any, ID any] struct {
	DB *bun.DB
}

func (b bunWrapper[R, ID]) Count(query string, args ...any) int64 {
	var row int64
	err := b.DB.NewRaw(query, args...).Scan(context.Background(), &row)
	if err == nil {
		return row
	}
	return 0
}

func (b bunWrapper[R, ID]) SelsectBy(query string, args ...any) (R, error) {
	var row R
	err := b.DB.NewRaw(query, args...).Scan(context.Background(), &row)
	if err != nil {
		return row, err
	}
	return row, errors.New("not found")
}

func (b bunWrapper[R, ID]) SelectOne(query string, args ...any) (R, error) {
	var row R
	var rows []R
	err := b.DB.NewRaw(query, args...).Scan(context.Background(), &rows)
	if err != nil {
		return row, err
	}
	if len(rows) > 0 {
		return rows[0], nil
	}
	return row, errors.New("not found")
}

func (b bunWrapper[R, ID]) SelectList(query string, args ...any) ([]R, error) {
	var rows []R
	err := b.DB.NewRaw(query, args...).Scan(context.Background(), &rows)
	if err != nil {
		return []R{}, err
	}
	if len(rows) > 0 {
		return rows, nil
	}
	return []R{}, nil
}

func (b bunWrapper[R, ID]) InsertTx(data *R, id *ID, commit ...bool) (*bun.Tx, error) {
	ctx := context.Background()
	tx, err := b.DB.Begin()
	if err != nil {
		return &tx, err
	}
	err = tx.NewInsert().Model(data).Returning("id").Scan(ctx, id)
	if err == nil {
		if id != nil {
			if len(commit) > 0 {
				if tx.Commit() == nil {
					return &tx, nil
				}
			}
			return &tx, nil
		}
		return &tx, tx.Rollback()
	}
	return &tx, err
}

func (b bunWrapper[R, ID]) UpdateTx(table string, value map[string]any, wheres string, args []any, commit ...bool) (*bun.Tx, error) {
	tx, err := b.DB.Begin()
	if err != nil {
		return &tx, err
	}
	if len(wheres) == 0 {
		return &tx, errors.New("there is no data to update")
	}

	rs, err := tx.NewUpdate().Table(table).Model(&value).Where(wheres, args...).Exec(context.Background())
	if err != nil {
		return &tx, err
	}

	_, err = rs.RowsAffected()
	if err != nil {
		return &tx, err
	}

	if len(commit) > 0 {
		if tx.Commit() == nil {
			return &tx, nil
		}
	}

	return &tx, nil
}

func (b bunWrapper[R, ID]) DeleteTx(table string, wheres string, args []any, commit ...bool) (*bun.Tx, error) {
	ctx := context.Background()
	tx, err := b.DB.Begin()
	if err != nil {
		return &tx, err
	}
	rs, err := tx.NewDelete().Table(table).Where(wheres, args...).Exec(ctx)
	if err == nil {
		row, e := rs.RowsAffected()
		if e != nil {
			return &tx, e
		}
		if row <= 0 {
			return &tx, tx.Rollback()
		}
	}

	if len(commit) > 0 {
		if tx.Commit() == nil {
			return &tx, nil
		}
	}

	return &tx, nil
}

func New[R any, ID any](db *bun.DB) bunWrapper[R, ID] {
	return bunWrapper[R, ID]{
		DB: db,
	}
}
