<?php
/**
 * Created by PhpStorm.
 * User: jumby
 * Date: 2016/10/11
 * Time: 9:34
 */

namespace App\Repositories\Traits;

use App\Models\Expression\Expression;
use DB;

trait MySqlPlusTrait
{
    protected $table;

    /**
     * Insert using mysql REPLACE INTO.
     *
     * @param array $values
     *
     * @return bool
     */
    public function replace(array $values)
    {
        if (empty($values)) {
            return true;
        }

        // Since every insert gets treated like a batch insert, we will make sure the
        // bindings are structured in a way that is convenient for building these
        // inserts statements by verifying the elements are actually an array.
        if (!is_array(reset($values))) {
            $values = [$values];
        } else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }
        // We'll treat every insert like a batch insert so we can easily insert each
        // of the records into the database consistently. This will make it much
        // easier on the grammars to just handle one type of record insertion.
        $bindings = [];

        foreach ($values as $record) {
            foreach ($record as $value) {
                $bindings[] = $value;
            }
        }

        $sql = $this->compileReplace($this->getTable(), $values);
        $bindings = $this->cleanBindings($bindings);

        return DB::statement($sql, $bindings);
    }

    /**
     * Compile an replace statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder $query
     * @param  array $values
     * @return string
     */
    public function compileReplace($table, array $values)
    {
        // Essentially we will force every insert to be treated as a batch insert which
        // simply makes creating the SQL easier for us since we can utilize the same
        // basic routine regardless of an amount of records given to us to insert.
        $table = $this->wrap($table);

        if (!is_array(reset($values))) {
            $values = [$values];
        }

        $columns = $this->columnize(array_keys(reset($values)));

        // We need to build a list of parameter place-holders of values that are bound
        // to the query. Each insert should have the exact same amount of parameter
        // bindings so we will loop through the record and parameterize them all.
        $parameters = [];

        foreach ($values as $record) {
            $parameters[] = '(' . $this->parameterize($record) . ')';
        }

        $parameters = implode(', ', $parameters);

        return "replace into $table ($columns) values $parameters";
    }

    /**
     * Wrap a value in keyword identifiers.
     *
     * @param  \Illuminate\Database\Query\Expression|string $value
     * @param  bool $prefixAlias
     * @return string
     */
    public function wrap($value)
    {
        $wrapped = [];
        $segments = explode('.', $value);
        // If the value is not an aliased table expression, we'll just wrap it like
        // normal, so if there is more than one segment, we will wrap the first
        // segments as if it was a table and the rest as just regular values.
        foreach ($segments as $key => $segment) {
            $wrapped[] = $this->wrapValue($segment);
        }

        return implode('.', $wrapped);
    }

    /**
     * Wrap a single string in keyword identifiers.
     *
     * @param  string $value
     * @return string
     */
    protected function wrapValue($value)
    {
        if ($value === '*') {
            return $value;
        }

        return '`' . str_replace('`', '``', $value) . '`';
    }

    /**
     * Convert an array of column names into a delimited string.
     *
     * @param  array $columns
     * @return string
     */
    public function columnize(array $columns)
    {
        return implode(', ', array_map([$this, 'wrap'], $columns));
    }

    /**
     * Create query parameter place-holders for an array.
     *
     * @param  array $values
     * @return string
     */
    public function parameterize(array $values)
    {
        return implode(', ', array_map([$this, 'parameter'], $values));
    }

    /**
     * Get the model table name
     *
     * @return mixed
     */
    public function getTable()
    {
        return $this->table;
    }

    /**
     * set the model table name
     *
     * @param mixed $table
     *
     * @return $this
     */
    public function setTable($table)
    {
        $this->table = $table;
        return $this;
    }

    /**
     * Insert using mysql on duplicate key update.
     *
     * @param array $values
     *
     * @return bool
     */
    public function insertOnDuplicate($values)
    {
        if (empty($values)) {
            return true;
        }

        // Since every insert gets treated like a batch insert, we will make sure the
        // bindings are structured in a way that is convenient for building these
        // inserts statements by verifying the elements are actually an array.
        if (!is_array(reset($values))) {
            $values = [$values];
        } else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }
        // We'll treat every insert like a batch insert so we can easily insert each
        // of the records into the database consistently. This will make it much
        // easier on the grammars to just handle one type of record insertion.
        $bindings = [];

        foreach ($values as $record) {
            foreach ($record as $value) {
                $bindings[] = $value;
            }
        }

        $sql = $this->compileInsertOnDuplicate($this->getTable(), $values);
        $bindings = $this->cleanBindings($bindings);

        return DB::statement($sql, $bindings);
    }

    /**
     * Compile an duplicate statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder $query
     *
     * @param  array $values
     *
     * @return string
     */
    public function compileInsertOnDuplicate($table, $values)
    {
        // Essentially we will force every insert to be treated as a batch insert which
        // simply makes creating the SQL easier for us since we can utilize the same
        // basic routine regardless of an amount of records given to us to insert.
        $table = $this->wrap($table);

        if (!is_array(reset($values))) {
            $values = [$values];
        }

        $columns = $this->columnize(array_keys(reset($values)));

        // We need to build a list of parameter place-holders of values that are bound
        // to the query. Each insert should have the exact same amount of parameter
        // bindings so we will loop through the record and parameterize them all.
        $parameters = [];

        foreach ($values as $record) {
            $parameters[] = '(' . $this->parameterize($record) . ')';
        }

        $duplicates = $this->duplicatize($this->values($columns));


        $parameters = implode(', ', $parameters);

        return "insert into $table ($columns) values $parameters $duplicates";
    }

    /**
     * convert columns to on duplicate key update
     *
     * @param $columns
     *
     * @return string
     */
    public function duplicatize($columns)
    {
        if (empty($columns)) {
            return '';
        }
        $duplicates = [];
        foreach ($columns as $column) {
            //skip the primary key
            if ($this->isPrimaryKey($column)) {
                continue;
            }
            $duplicates[] = "$column=values($column)";
        }
        return 'on duplicate key update ' . implode(', ', $duplicates);
    }

    /**
     * Check the primary key
     *
     * @param string $column
     *
     * @return bool
     */
    public function isPrimaryKey($column)
    {
        $key = $this->columnize([$this->getModel()->getKeyName()]);
        if ($key === $column) {
            return true;
        }
        return false;
    }

    /**
     * Convert string columns to array
     *
     * @param $columns
     *
     * @return array
     */
    protected function values($columns)
    {
        return explode(', ', $columns);
    }

    /**
     *  Insert using mysql ignore.
     *
     * @param $values
     *
     * @return bool
     */
    public function insertIgnore($values)
    {
        if (empty($values)) {
            return true;
        }

        // Since every insert gets treated like a batch insert, we will make sure the
        // bindings are structured in a way that is convenient for building these
        // inserts statements by verifying the elements are actually an array.
        if (!is_array(reset($values))) {
            $values = [$values];
        } else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }
        // We'll treat every insert like a batch insert so we can easily insert each
        // of the records into the database consistently. This will make it much
        // easier on the grammars to just handle one type of record insertion.
        $bindings = [];

        foreach ($values as $record) {
            foreach ($record as $value) {
                $bindings[] = $value;
            }
        }

        $sql = $this->compileIgnore($this->getTable(), $values);
        $bindings = $this->cleanBindings($bindings);

        return DB::statement($sql, $bindings);
    }

    /**
     * Compile an insert statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder $query
     * @param  array $values
     * @return string
     */
    public function compileIgnore($table, array $values)
    {
        // Essentially we will force every insert to be treated as a batch insert which
        // simply makes creating the SQL easier for us since we can utilize the same
        // basic routine regardless of an amount of records given to us to insert.
        $table = $this->wrap($table);

        if (!is_array(reset($values))) {
            $values = [$values];
        }

        $columns = $this->columnize(array_keys(reset($values)));

        // We need to build a list of parameter place-holders of values that are bound
        // to the query. Each insert should have the exact same amount of parameter
        // bindings so we will loop through the record and parameterize them all.
        $parameters = [];

        foreach ($values as $record) {
            $parameters[] = '(' . $this->parameterize($record) . ')';
        }

        $parameters = implode(', ', $parameters);

        return "insert ignore into $table ($columns) values $parameters";
    }

    /**
     * Get the appropriate query parameter place-holder for a value.
     *
     * @param  mixed $value
     * @return string
     */
    protected function parameter()
    {
        return '?';
    }

    /**
     * Remove all of the expressions from a list of bindings.
     *
     * @param  array $bindings
     * @return array
     */
    protected function cleanBindings(array $bindings)
    {
        return array_values(array_filter($bindings, function ($binding) {
            return !$binding instanceof Expression;
        }));
    }

}
