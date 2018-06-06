<?php
/**
 * Created by IntelliJ IDEA.
 * User: valentin
 * Date: 28/01/2018
 * Time: 16:42
 */

namespace Phinx\Db\Adapter;


use Exception;
use Phinx\Db\Table;
use Phinx\Db\Table\Column;
use Phinx\Db\Table\ForeignKey;
use Phinx\Db\Table\Index;

class FirebirdAdapter extends PdoAdapter implements AdapterInterface
{

    const INT_SMALL = 32767;
    const COMMENT_TYPE_TABLE = 'TABLE';
    const COMMENT_TYPE_COLUMN = 'COLUMN';

    /**
     * {@inheritdoc}
     */
    public function connect()
    {
        if ($this->connection === null) {
            if (!class_exists('PDO') || !in_array('firebird', \PDO::getAvailableDrivers(), true)) {
                // try our connection via freetds (Mac/Linux)
                $this->connectDblib();

                return;
            }

            $db = null;
            $options = $this->getOptions();
            $options += [
                'port' => 3050,
                'charset' => 'utf8'
            ];

            $dsn = 'firebird:dbname=' . $options['host'] . '/' . $options['port'] . ':' . $options['name'] . ';charset=' . $options['charset'];

            try {
                $db = new \PDO($dsn, $options['user'], $options['pass']);
            } catch (\PDOException $exception) {
                throw new \InvalidArgumentException(sprintf(
                    'There was a problem connecting to the database: %s',
                    $exception->getMessage()
                ));
            }

            $this->setConnection($db);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function disconnect()
    {
        $this->connection = null;
    }

    /**
     * {@inheritdoc}
     */
    public function hasTransactions()
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    public function beginTransaction()
    {
        $this->execute('SET TRANSACTION');
    }

    /**
     * {@inheritdoc}
     */
    public function commitTransaction()
    {
        $this->execute('COMMIT');
    }

    /**
     * {@inheritdoc}
     */
    public function rollbackTransaction()
    {
        $this->execute('ROLLBACK');
    }

    /**
     * {@inheritdoc}
     */
    public function quoteTableName($tableName)
    {
        return str_replace('.', '"."', $this->quoteColumnName($tableName));
    }

    /**
     * {@inheritdoc}
     */
    public function quoteColumnName($columnName)
    {
        return '"' . $columnName . '"';
    }

    /**
     * {@inheritdoc}
     */
    public function hasTable($tableName)
    {
        $sql = sprintf(
            'SELECT RDB$RELATION_NAME
                  FROM RDB$RELATIONS
                  WHERE RDB$SYSTEM_FLAG = 0 AND RDB$RELATION_NAME = \'%s\'',
            $tableName
        );

        $exists = $this->fetchRow($sql);

        return !empty($exists);
    }

    /**
     * {@inheritdoc}
     */
    public function createTable(Table $table)
    {
        $defaultOptions = [
            'charset' => 'utf8',
            'collation' => 'utf8'
        ];

        $options = array_merge(
            $defaultOptions,
            array_intersect_key($this->getOptions(), $defaultOptions),
            $table->getOptions()
        );

        // Add default primary key
        $columns = $table->getPendingColumns();
        if (!isset($options['id']) || (isset($options['id']) && $options['id'] === true)) {
            $column = new Column();
            $column->setName('id')
                ->setType('integer')
                ->setSigned(isset($options['signed']) ? $options['signed'] : true)
                ->setIdentity(true);

            array_unshift($columns, $column);
            $options['primary_key'] = 'id';
        } elseif (isset($options['id']) && is_string($options['id'])) {
            // Handle id => "field_name" to support AUTO_INCREMENT
            $column = new Column();
            $column->setName($options['id'])
                ->setType('integer')
                ->setIdentity(true);

            array_unshift($columns, $column);
            $options['primary_key'] = $options['id'];
        }

        $comments = [];
        $sql = sprintf('CREATE TABLE %s (', $this->quoteTableName($table->getName()));
        foreach ($columns as $column) {
            if (in_array(
                $column->getType(),
                [
                    AdapterInterface::PHINX_TYPE_CHAR,
                    AdapterInterface::PHINX_TYPE_STRING,
                    AdapterInterface::PHINX_TYPE_VARBINARY,
                    AdapterInterface::PHINX_TYPE_TEXT
                ]
            )) {
                // Firebird encoding and collation is on field not on table
                if (!$column->getEncoding()) $column->setEncoding($options['charset']);
                if (!$column->getCollation()) $column->setCollation($options['collation']);
            }

            $sql .= $this->quoteColumnName($column->getName()) . ' ' . $this->getColumnSqlDefinition($column) . ', ';
            if ($column->getComment()) {
                $comments[] = [
                    self::COMMENT_TYPE_COLUMN,
                    $table->getName() . '.' . $column->getName(),
                    $column->getComment()
                ];
            }
        }

        // set the primary key(s)
        if (isset($options['primary_key'])) {
            $sql = rtrim($sql);
            $sql .= ' PRIMARY KEY (';
            if (is_string($options['primary_key'])) { // handle primary_key => 'id'
                $sql .= $this->quoteColumnName($options['primary_key']);
            } elseif (is_array($options['primary_key'])) { // handle primary_key => array('tag_id', 'resource_id')
                $sql .= implode(',', array_map([$this, 'quoteColumnName'], $options['primary_key']));
            }
            $sql .= ')';
        } else {
            $sql = substr(rtrim($sql), 0, -1); // no primary keys
        }

        // set the foreign keys
        $foreignKeys = $table->getForeignKeys();
        foreach ($foreignKeys as $foreignKey) {
            $sql .= ', ' . $this->getForeignKeySqlDefinition($foreignKey, $table->getName());
        }

        $sql .= ');';

        // execute the sql
        $this->execute($sql);

        // add index
        $indexes = $table->getIndexes();
        foreach ($indexes as $index) {
            $this->addIndex($table, $index);
        }

        // add comment
        if (isset($options['comment'])) {
            $comments[] = [
                self::COMMENT_TYPE_TABLE,
                $table->getName(),
                $options['comment']
            ];
        }

        // Apply comments
        if (!empty($comments)) {
            foreach ($comments as $comment) {
                $this->comment($comment[0], $comment[1], $comment[2]);
            }
        }
    }

    /**
     * {@inheritdoc}
     * @see http://www.firebirdfaq.org/faq363/
     */
    public function renameTable($tableName, $newName)
    {
        throw new Exception("Firebird can't rename table");
    }

    /**
     * {@inheritdoc}
     */
    public function dropTable($tableName)
    {
        $this->execute(sprintf('DROP TABLE %s', $this->quoteTableName($tableName)));
    }

    /**
     * {@inheritdoc}
     */
    public function truncateTable($tableName)
    {
        $this->execute(sprintf('DELETE FROM %s', $this->quoteTableName($tableName)));
    }

    /**
     * {@inheritdoc}
     */
    public function getColumns($tableName)
    {
        $columns = [];
        $sql = 'SELECT TRIM(r.rdb$field_name) AS "field_name",
                    TRIM(CASE f.rdb$field_type
                        WHEN 261 THEN CASE f.rdb$field_sub_type
                                        WHEN 0 THEN \'blob\'
                                        WHEN 1 THEN \'blobstring\'
                                      END
                        WHEN 14 THEN \'char\'
                        WHEN 27 THEN \'double\'
                        WHEN 10 THEN \'float\'
                        WHEN 16 THEN CASE f.rdb$field_sub_type
                                       WHEN 1 THEN \'numeric\'
                                       WHEN 2 THEN \'decimal\'
                                       ELSE \'bigint\'
                                     END
                        WHEN 8 THEN \'integer\'
                        WHEN 7 THEN \'smallint\'
                        WHEN 12 THEN \'date\'
                        WHEN 13 THEN \'time\'
                        WHEN 35 THEN \'timestamp\'
                        WHEN 37 THEN \'varchar\'
                        WHEN 23 THEN \'boolean\'
                        ELSE \'UNKNOWN\'
                      END) AS "field_type",
                      r.rdb$null_flag AS "field_null", TRIM(r.rdb$default_source) AS "field_default",
                      IIF(f.rdb$computed_source IS NULL, f.rdb$character_length, f.rdb$field_length / cset.RDB$BYTES_PER_CHARACTER) AS "field_charlength",
                      r.rdb$identity_type AS field_identity, r.rdb$description AS "field_comment",
                      coll.rdb$collation_name AS "field_collation",
                      f.rdb$field_precision AS "field_precision", IIF(f.rdb$field_scale IS NULL, NULL, f.rdb$field_scale * -1) AS "field_scale",
                      f.rdb$field_sub_type AS field_subtype, IIF(g.rdb$system_flag = 6, \'AUTO_INCREMENT\', \'\') AS "field_extra"
                FROM rdb$relation_fields r
                LEFT JOIN rdb$fields f ON r.rdb$field_source = f.rdb$field_name
                LEFT JOIN rdb$collations coll ON f.rdb$collation_id = coll.rdb$collation_id AND f.rdb$character_set_id = coll.rdb$character_set_id
                LEFT JOIN rdb$character_sets cset ON f.rdb$character_set_id = cset.rdb$character_set_id
                LEFT JOIN rdb$generators g on r.rdb$generator_name = g.rdb$generator_name
                WHERE r.rdb$relation_name = \'%s\'
                ORDER BY r.rdb$field_position';

        $rows = $this->fetchAll(sprintf($sql, $tableName));
        foreach ($rows as $columnInfo) {
            $column = new Column();
            $type = $this->getPhinxType($columnInfo['field_type']);

            $column
                ->setName($columnInfo['field_name'])
                ->setType($type)
                ->setNull($columnInfo['field_null'] === '1')
                ->setDefault($this->parseDefault($type, $columnInfo['field_default']))
                ->setIdentity($columnInfo['field_extra'] === 'AUTO_INCREMENT')
                ->setComment($columnInfo['field_comment']);

            if (!empty($columnInfo['field_collation'])) {
                $column->setCollation($columnInfo['field_collation']);
            }
            if (!empty($columnInfo['field_charlength'])) {
                $column->setLimit($columnInfo['field_charlength']);
            }
            if (!empty($columnInfo['field_precision'])) {
                $column->setPrecision($columnInfo['field_precision']);
            }
            if (!empty($columnInfo['field_scale'])) {
                $column->setScale($columnInfo['field_scale']);
            }

            $columns[$columnInfo['field_name']] = $column;
        }

        return $columns;
    }

    /**
     * {@inheritdoc}
     */
    public function hasColumn($tableName, $columnName)
    {
        $sql = sprintf(
            'SELECT COUNT(*)
                    FROM rdb$relation_fields f
                    WHERE f.rdb$relation_name = \'%s\' AND f.rdb$field_name = \'%s\'',
            $tableName,
            $columnName
        );

        return $this->fetchRow($sql)[0] > 0;
    }

    /**
     * {@inheritdoc}
     */
    public function addColumn(Table $table, Column $column)
    {
        $sql = sprintf(
            'ALTER TABLE %s ADD %s %s',
            $this->quoteTableName($table->getName()),
            $this->quoteColumnName($column->getName()),
            $this->getColumnSqlDefinition($column)
        );
        $this->execute($sql);

        if ($column->getAfter()) {
            $this->changeColumnPosition($table, $column, $column->getAfter());
        }

    }

    /**
     * {@inheritdoc}
     */
    public function renameColumn($tableName, $columnName, $newColumnName)
    {
        if (!$this->hasColumn($tableName, $columnName)) {
            throw new \InvalidArgumentException("The specified column does not exist: $columnName");
        }

        $this->execute(
            sprintf(
                'ALTER TABLE %s ALTER COLUMN %s TO %s',
                $this->quoteTableName($tableName),
                $this->quoteColumnName($columnName),
                $this->quoteColumnName($newColumnName)
            )
        );
    }

    /**
     * {@inheritdoc}
     */
    public function changeColumn($tableName, $columnName, Column $newColumn)
    {
        if ($columnName !== $newColumn->getName()) {
            $this->renameColumn($tableName, $columnName, $newColumn->getName());
        }

        $this->execute(
            sprintf(
                'ALTER TABLE %s ALTER COLUMN %s %s',
                $this->quoteTableName($tableName),
                $this->quoteColumnName($newColumn->getName()),
                $this->getColumnSqlDefinition($newColumn, false)
            )
        );

        // change column default if needed or drop default
        if (!is_null($newColumn->getDefault())) {
            $this->execute(
                sprintf(
                    'ALTER TABLE %s ALTER COLUMN %s SET %s',
                    $this->quoteTableName($tableName),
                    $this->quoteColumnName($newColumn->getName()),
                    $this->getDefaultValueDefinition($newColumn->getDefault())
                )
            );
        } else {
            $this->execute(
                sprintf(
                    'ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT',
                    $this->quoteTableName($tableName),
                    $this->quoteColumnName($newColumn->getName())
                )
            );
        }

        // change column comment if needed
        if ($newColumn->getComment()) {
            $this->comment('COLUMN', "$tableName.{$newColumn->getName()}", $newColumn->getComment());
        }
    }

    /**
     * {@inheritdoc}
     */
    public function dropColumn($tableName, $columnName)
    {
        $this->execute(
            sprintf(
                'ALTER TABLE %s DROP %s',
                $this->quoteTableName($tableName),
                $this->quoteColumnName($columnName)
            )
        );
    }

    /**
     * {@inheritdoc}
     */
    public function hasIndex($tableName, $columns)
    {
        if (is_string($columns)) {
            $columns = [$columns]; // str to array
        }

        $columns = array_map('strtolower', $columns);
        $indexes = $this->getIndexes($tableName);

        foreach ($indexes as $index) {
            $a = array_diff($columns, $index['columns']);

            if (empty($a)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Get an array of indexes from a particular table.
     *
     * @param string $tableName Table Name
     * @return array
     */
    public function getIndexes($tableName)
    {
        $indexes = [];
        $sql = sprintf('SELECT TRIM(i.rdb$index_name) AS "index_name", TRIM(iss.rdb$field_name) AS "column_name"
                 FROM rdb$indices i
                 INNER JOIN rdb$index_segments iss ON i.rdb$index_name = iss.rdb$index_name
                 WHERE i.rdb$relation_name =\'%s\'
                   AND i.rdb$foreign_key IS NULL', $tableName);

        $rows = $this->fetchAll($sql);
        foreach ($rows as $row) {
            if (!isset($indexes[$row['index_name']])) {
                $indexes[$row['index_name']] = ['columns' => []];
            }
            $indexes[$row['index_name']]['columns'][] = strtolower($row['column_name']);
        }

        return $indexes;
    }

    /**
     * {@inheritdoc}
     */
    public function hasIndexByName($tableName, $indexName)
    {
        $indexes = $this->getIndexes($tableName);

        foreach ($indexes as $name => $index) {
            if ($name === $indexName) {
                return true;
            }
        }

        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function addIndex(Table $table, Index $index)
    {
        $name = ($index->getName() ?: 'idx_' . $table->getName() . '_' . implode('_', $index->getColumns()));
        $columns = array_map([$this, 'quoteColumnName'], $index->getColumns());

        if (!$this->checkIndexLength($table->getName(), $index->getColumns())) {
            throw new Exception("Firebird index segment is too long");
        }

        $this->execute(
            sprintf(
                'CREATE %sINDEX %s ON %s %s',
                $index->getType() === Index::UNIQUE ? 'UNIQUE ' : '',
                $this->quoteTableName($name),
                $this->quoteTableName($table->getName()),
                '(' . implode(', ', $columns) . ')'
            )
        );
    }

    /**
     * {@inheritdoc}
     */
    public function dropIndex($tableName, $columns)
    {
        if (is_string($columns)) {
            $columns = [$columns]; // str to array
        }

        $indexes = $this->getIndexes($tableName);
        $columns = array_map('strtolower', $columns);

        foreach ($indexes as $indexName => $index) {
            if ($columns == $index['columns']) {
                $this->execute(sprintf('DROP INDEX %s', $this->quoteColumnName($indexName)));
                return;
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    public function dropIndexByName($tableName, $indexName)
    {
        $this->execute(sprintf('DROP INDEX %s', $this->quoteTableName($indexName)));
    }

    /**
     * {@inheritdoc}
     */
    public function hasForeignKey($tableName, $columns, $constraint = null)
    {
        if (is_string($columns)) {
            $columns = [$columns]; // str to array
        }
        $foreignKeys = $this->getForeignKeys($tableName);
        if ($constraint) {
            if (isset($foreignKeys[$constraint])) {
                return !empty($foreignKeys[$constraint]);
            }

            return false;
        } else {
            foreach ($foreignKeys as $key) {
                if ($columns == $key['columns']) {
                    return true;
                }
            }

            return false;
        }
    }

    private function getForeignKeys($tableName) {
        $foreignKeys = [];

        $sql = sprintf('SELECT
                                TRIM(RC.RDB$CONSTRAINT_NAME)  AS "constraint_name",
                                TRIM(FK.RDB$RELATION_NAME)    AS "table_name",
                                TRIM(ISF.RDB$FIELD_NAME)      AS "column_name",
                                TRIM(PK.RDB$RELATION_NAME)    AS "referenced_table_name",
                                TRIM(ISP.RDB$FIELD_NAME)      AS "referenced_column_name"
                            FROM        RDB$REF_CONSTRAINTS         RC
                            INNER JOIN  RDB$RELATION_CONSTRAINTS    PK  ON PK.RDB$CONSTRAINT_NAME   = RC.RDB$CONST_NAME_UQ
                            INNER JOIN  RDB$INDEX_SEGMENTS          ISP ON ISP.RDB$INDEX_NAME       = PK.RDB$INDEX_NAME
                            INNER JOIN  RDB$RELATION_CONSTRAINTS    FK  ON FK.RDB$CONSTRAINT_NAME   = RC.RDB$CONSTRAINT_NAME    AND ISP.RDB$INDEX_NAME      = PK.RDB$INDEX_NAME
                            INNER JOIN  RDB$INDEX_SEGMENTS          ISF ON ISF.RDB$INDEX_NAME       = FK.RDB$INDEX_NAME         AND ISP.RDB$FIELD_POSITION  = ISF.RDB$FIELD_POSITION
                            WHERE FK.RDB$CONSTRAINT_TYPE = \'FOREIGN KEY\' AND FK.RDB$RELATION_NAME = \'%s\'
                            ORDER BY ISF.RDB$FIELD_POSITION', $tableName);
        $rows = $this->fetchAll($sql);

        foreach ($rows as $row) {
            $foreignKeys[$row['constraint_name']]['table'] = $row['table_name'];
            $foreignKeys[$row['constraint_name']]['columns'][] = $row['column_name'];
            $foreignKeys[$row['constraint_name']]['referenced_table'] = $row['referenced_table_name'];
            $foreignKeys[$row['constraint_name']]['referenced_columns'][] = $row['referenced_column_name'];
        }

        return $foreignKeys;
    }

    /**
     * Adds the specified foreign key to a database table.
     *
     * @param \Phinx\Db\Table $table
     * @param \Phinx\Db\Table\ForeignKey $foreignKey
     * @return void
     */
    public function addForeignKey(Table $table, ForeignKey $foreignKey)
    {
        $this->execute(
            sprintf(
                'ALTER TABLE %s ADD %s',
                $this->quoteTableName($table->getName()),
                $this->getForeignKeySqlDefinition($foreignKey, $table->getName())
            )
        );
    }

    /**
     * {@inheritdoc}
     */
    public function dropForeignKey($tableName, $columns, $constraint = null)
    {
        if (is_string($columns)) {
            $columns = [$columns]; // str to array
        }

        if ($constraint) {
            $this->execute(
                sprintf(
                    'ALTER TABLE %s DROP CONSTRAINT %s',
                    $this->quoteTableName($tableName),
                    $this->quoteTableName($constraint)
                )
            );

            return;
        } else {
            foreach ($columns as $column) {
                $rows = $this->fetchAll(sprintf(
                    'SELECT
                                TRIM(RC.RDB$CONSTRAINT_NAME)  AS "constraint_name",
                                TRIM(FK.RDB$RELATION_NAME)    AS "table_name",
                                TRIM(ISF.RDB$FIELD_NAME)      AS "column_name",
                                TRIM(PK.RDB$RELATION_NAME)    AS "referenced_table_name",
                                TRIM(ISP.RDB$FIELD_NAME)      AS "referenced_column_name",
                                RC.RDB$UPDATE_RULE             AS UPDATE_RULE,
                                RC.RDB$DELETE_RULE             AS DELETE_RULE
                            FROM        RDB$REF_CONSTRAINTS         RC
                            INNER JOIN  RDB$RELATION_CONSTRAINTS    PK  ON PK.RDB$CONSTRAINT_NAME   = RC.RDB$CONST_NAME_UQ
                            INNER JOIN  RDB$INDEX_SEGMENTS          ISP ON ISP.RDB$INDEX_NAME       = PK.RDB$INDEX_NAME
                            INNER JOIN  RDB$RELATION_CONSTRAINTS    FK  ON FK.RDB$CONSTRAINT_NAME   = RC.RDB$CONSTRAINT_NAME    AND ISP.RDB$INDEX_NAME      = PK.RDB$INDEX_NAME
                            INNER JOIN  RDB$INDEX_SEGMENTS          ISF ON ISF.RDB$INDEX_NAME       = FK.RDB$INDEX_NAME         AND ISP.RDB$FIELD_POSITION  = ISF.RDB$FIELD_POSITION
                            WHERE FK.RDB$CONSTRAINT_TYPE = \'FOREIGN KEY\' AND FK.RDB$RELATION_NAME = \'%s\' AND ISF.RDB$FIELD_NAME = \'%s\'
                            ORDER BY ISF.RDB$FIELD_POSITION',
                    $tableName,
                    $column
                ));
                foreach ($rows as $row) {
                    $this->dropForeignKey($tableName, $columns, $row['constraint_name']);
                }
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    public function getSqlType($type, $limit = null)
    {
        switch ($type) {
            case static::PHINX_TYPE_STRING:
                return ['name' => 'varchar', 'limit' => $limit ?: 200];
            case static::PHINX_TYPE_CHAR:
            case static::PHINX_TYPE_VARBINARY:
                return ['name' => 'char', 'limit' => $limit ?: 200];
            case static::PHINX_TYPE_TEXT:
            case static::PHINX_TYPE_JSON:
            case static::PHINX_TYPE_BLOB:
            case static::PHINX_TYPE_BINARY:
                $subType = '';
                $subTypesMap = [
                    'text' => [
                        static::PHINX_TYPE_TEXT,
                        static::PHINX_TYPE_JSON
                    ],
                    'binary' => [
                        static::PHINX_TYPE_BLOB,
                        static::PHINX_TYPE_BINARY
                    ]
                ];

                foreach ($subTypesMap as $subTypeKey => $subTypeMap) {
                    if (in_array($type, $subTypeMap)) {
                        $subType = $subTypeKey;
                        break;
                    }
                }

                return ['name' => 'blob sub_type ' . $subType];
            case static::PHINX_TYPE_INTEGER:
                if ($limit === self::INT_SMALL) {
                    return ['name' => 'smallint'];
                }
                return ['name' => 'integer'];
            case static::PHINX_TYPE_BIG_INTEGER:
                return ['name' => 'bigint'];
            case static::PHINX_TYPE_FLOAT:
                return ['name' => 'float'];
            case static::PHINX_TYPE_DECIMAL:
                return ['name' => 'decimal'];
            case static::PHINX_TYPE_DATETIME:
            case static::PHINX_TYPE_TIMESTAMP:
                return ['name' => 'timestamp'];
            case static::PHINX_TYPE_TIME:
                return ['name' => 'time'];
            case static::PHINX_TYPE_DATE:
                return ['name' => 'date'];
            case static::PHINX_TYPE_BOOLEAN:
                return ['name' => 'boolean'];
            case static::PHINX_TYPE_UUID:
                return ['name' => 'char', 'limit' => 38];
            default:
                throw new \RuntimeException('The type: "' . $type . '" is not supported.');
        }
    }

    /**
     * {@inheritdoc}
     */
    public function createDatabase($name, $options = [])
    {
        $options = array_merge($this->getOptions(), $options);

        $charset = isset($options['charset']) ? $options['charset'] : 'utf8';

        $sql = sprintf('CREATE DATABASE \'%s\'', $name, $charset);

        if (isset($options['page_size'])) {
            $sql .= sprintf(' PAGE_SIZE = %s', $options['page_size']);
        }

        $sql .= sprintf(' DEFAULT CHARACTER SET %s', $charset);
        if (isset($options['collation'])) {
            $sql .= sprintf(' COLLATION %s', $options['collation']);
        }

        $this->executeIsql($sql, false);
    }

    /**
     * Get the pageSize of database.
     *
     * @param $name String Database name
     * @return int PageSize of database
     */
    public function getPageSize($name) {
        return $this->wrapDatabaseOption($name, function () {
            $rows = $this->fetchAll('SELECT FIRST 1 mon$page_size AS "page_size" FROM MON$DATABASE');
            return $rows[0]['page_size'];
        });
    }

    /**
     * Checks to see if a database exists.
     *
     * @param string $name Database Name
     * @return bool
     */
    public function hasDatabase($name)
    {
        return $this->wrapDatabaseOption($name, function () {
            try {
                $this->connect();
                $this->disconnect();
                return true;
            } catch (\InvalidArgumentException $exception) {
                return false;
            }
        });
    }

    private function wrapDatabaseOption($name, $callback) {
        $options = $this->getOptions();
        $database = $options['name'];
        $options['name'] = $name;
        $this->setOptions($options);

        $result = call_user_func($callback);

        $options['name'] = $database;
        $this->setOptions($options);

        return $result;
    }

    /**
     * Drops the specified database.
     *
     * @param string $name Database Name
     * @return void
     */
    public function dropDatabase($name)
    {
        try {
            $this->wrapDatabaseOption($name, function () {
                $this->executeIsql('DROP DATABASE', true);
            });
        } catch (\Exception $exception) {
            // Database do not exists
        }
    }

    /**
     * Comment an object.
     *
     * @param $type String Object type.
     * @param $name String Object full name.
     * @param $comment String The comment.
     * @return void
     */
    protected function comment($type, $name, $comment) {
        $this->execute(sprintf(
            'COMMENT ON %s %s IS %s',
            $type,
            $this->quoteTableName($name),
            $this->getConnection()->quote($comment)
        ));
    }

    /**
     * Gets the Firebird Column Definition for a Column object.
     *
     * @param \Phinx\Db\Table\Column $column Column
     * @return string
     */
    private function getColumnSqlDefinition($column)
    {
        $sqlType = $this->getSqlType($column->getType(), $column->getLimit());

        $def = strtoupper($sqlType['name']);

        if ($column->getPrecision() && $column->getScale()) {
            $def .= '(' . $column->getPrecision() . ',' . $column->getScale() . ')';
        } elseif (isset($sqlType['limit'])) {
            $def .= '(' . $sqlType['limit'] . ')';
        }
        if (($values = $column->getValues()) && is_array($values)) {
            $def .= "('" . implode("', '", $values) . "')";
        }
        $def .= $column->getEncoding() ? ' CHARACTER SET ' . $column->getEncoding() : '';
        $def .= $this->getDefaultValueDefinition($column->getDefault());
        $def .= ($column->isIdentity()) ? ' GENERATED BY DEFAULT AS IDENTITY' : '';
        $def .= ($column->isNull() == false) ? ' NOT NULL' : '';
        $def .= $column->getCollation() ? ' COLLATE ' . $column->getCollation() : '';

        return $def;
    }

    private function getDefaultValueDefinition($default)
    {
        if (is_string($default) && 'CURRENT_TIMESTAMP' !== $default) {
            $default = $this->getConnection()->quote($default);
        } elseif (is_bool($default)) {
            $default = $this->castToBool($default);
        }

        return isset($default) ? ' DEFAULT ' . $default : '';
    }

    private function getForeignKeySqlDefinition(ForeignKey $foreignKey, $tableName)
    {
        $constraintName = $foreignKey->getConstraint() ?: $tableName . '_' . implode('_', $foreignKey->getColumns());
        $colums = array_map([$this, 'quoteColumnName'], $foreignKey->getColumns());
        $referencedColumns = array_map([$this, 'quoteColumnName'], $foreignKey->getReferencedColumns());

        $def = 'CONSTRAINT ' . $this->quoteColumnName($constraintName);
        $def .= ' FOREIGN KEY (' . implode(', ', $colums) . ')';
        $def .= " REFERENCES {$this->quoteTableName($foreignKey->getReferencedTable()->getName())} (" . implode(', ', $referencedColumns) . ')';

        if ($foreignKey->getOnDelete()) {
            $def .= " ON DELETE {$foreignKey->getOnDelete()}";
        }
        if ($foreignKey->getOnUpdate()) {
            $def .= " ON UPDATE {$foreignKey->getOnUpdate()}";
        }

        return $def;
    }

    protected function executeIsql($sql, $connect) {
        // Run isql
        $cmd = sprintf(
            'isql -user %s -password %s %s',
            $this->getOption('user'),
            $this->getOption('pass'),
            $connect ? $this->getOption('host') . ':' . $this->getOption('name') : ''
        );

        $proc = proc_open($cmd, [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);//, $cwd);

        fwrite($pipes[0], $sql . ';');
        fclose($pipes[0]);

        $result['stdout'] = stream_get_contents($pipes[1]);
        fclose($pipes[1]);
        $result['stderr'] = stream_get_contents($pipes[2]);
        fclose($pipes[2]);
        $result['return'] = proc_close($proc);

        if ($result['return'] !== 0) {
            throw new Exception('Exception in Isql, stderr : ' . $result['stderr']);
        }
    }

    private function changeColumnPosition(Table $table, Column $column, $after)
    {
        // get the after field position
        $rows = $this->fetchAll(
            sprintf(
                'SELECT rdb$field_position + 1 as "field_position"
                        FROM rdb$relation_fields
                        WHERE rdb$relation_name = \'%s\' AND rdb$field_name = \'%s\'',
                $table->getName(),
                $after
            )
        );
        $position = $rows[0]['field_position'] + 1;

        $this->execute(
            sprintf(
                'ALTER TABLE %s ALTER COLUMN %s POSITION %s',
                $this->quoteTableName($table->getName()),
                $this->quoteColumnName($column->getName()),
                $position
            )
        );
    }

    private function getPhinxType($sqlType)
    {
        switch ($sqlType) {
            case 'varchar':
                return static::PHINX_TYPE_STRING;
            case 'char':
                return static::PHINX_TYPE_CHAR;
            case 'smallint':
            case 'integer':
                return static::PHINX_TYPE_INTEGER;
            case 'decimal':
            case 'numeric':
                return static::PHINX_TYPE_DECIMAL;
            case 'bigint':
                return static::PHINX_TYPE_BIG_INTEGER;
            case 'float':
                return static::PHINX_TYPE_FLOAT;
            case 'time':
                return static::PHINX_TYPE_TIME;
            case 'date':
                return static::PHINX_TYPE_DATE;
            case 'timestamp':
                return static::PHINX_TYPE_DATETIME;
            case 'boolean':
                return static::PHINX_TYPE_BOOLEAN;
            case 'blob':
                return static::PHINX_TYPE_BLOB;
            case 'blobstring':
                return static::PHINX_TYPE_TEXT;
            default:
                throw new \RuntimeException('The Firebird type: "' . $sqlType . '" is not supported');
        }
    }

    /**
     * Parse default source of field.
     *
     * @param $type String PHINX_TYPE const
     * @param $field_default String The default value from statement
     * @return mixed The default value
     */
    public function parseDefault($type, $field_default)
    {
        if (is_null($field_default)) return null;

        $pattern = '/DEFAULT %s/';
        $map = [
            [
                'types' => [
                    static::PHINX_TYPE_BIG_INTEGER,
                    static::PHINX_TYPE_INTEGER,
                    static::PHINX_TYPE_DECIMAL,
                    static::PHINX_TYPE_FLOAT,
                    static::PHINX_TYPE_BOOLEAN,
                ],
                'pattern' => '(.*)'
            ],
            [
                'types' => [
                    static::PHINX_TYPE_STRING,
                    static::PHINX_TYPE_CHAR,
                    static::PHINX_TYPE_TEXT,
                ],
                'pattern' => '\'(.*?)\''
            ]
        ];

        foreach ($map as $test) {
            if (in_array($type, $test['types'])) {
                $pattern = sprintf($pattern, $test['pattern']);
                break;
            }
        }

        preg_match($pattern, $field_default, $matches);

        return $matches[1];
    }

    /**
     * Override blukinsert because Firebird don't support multiple insert like 'INSERT INTO tableName VALUEs (?, ?), (?, ?)'.
     * Also support multiple insert statement into a execute block.
     * @see http://www.firebirdfaq.org/faq336/
     *
     * @param \Phinx\Db\Table $table where to insert data
     * @param array $rows
     * @return void
     */
    public function bulkinsert(Table $table, $rows)
    {
        $quotedTableName = $this->quoteTableName($table->getName());
        $insertSql = sprintf(
            "INSERT INTO %s ",
            $quotedTableName
        );

        $current = current($rows);
        $keys = array_keys($current);
        $insertSql .= '(' . implode(', ', array_map([$this, 'quoteColumnName'], $keys)) . ') VALUES ';
        $paramBuilder = function ($key) use ($quotedTableName) {
            return sprintf(
                '%s TYPE OF COLUMN %s.%s = ?',
                key($key),
                $quotedTableName,
                $this->quoteColumnName(current($key))
            );
        };

        $vals = [];
        $params = [];
        $queries = [];
        foreach ($rows as $index => $row) {
            $queries[] = $insertSql . '(' . implode(', ', array_map(function ($key) use ($index) { return ':' . $key . $index; }, $keys)) . ');';
            foreach ($row as $key => $v) {
                $params[] = [$key . $index => $key];
                $vals[] = $v;
            }
        }

        $sql = sprintf(
            'EXECUTE BLOCK %s AS BEGIN /* \' */ %s END;',
            '(' . implode(', ', array_map($paramBuilder, $params)) . ')',
            implode(' ', $queries)
        );

        $stmt = $this->getConnection()->prepare($sql);
        $stmt->execute($vals);
    }

    /**
     * Check length of string fields because can't more than ((page_size / 4 - 9)), it's Firebird limitation.
     * @see https://www.firebirdsql.org/refdocs/langrefupd20-create-index.html#langrefupd20-creatind-keylength
     * @see http://www.firebirdfaq.org/faq211/
     *
     * @param $tablename string The table name
     * @param $columns array The index columns
     * @return boolean Is valid
     */
    private function checkIndexLength($tablename, $columns)
    {
        $pageSize = $this->fetchRow('SELECT MON$PAGE_SIZE / 4 - 4 AS "page_size" FROM MON$DATABASE')['page_size'];
        $rows = $this->fetchAll(
            sprintf(
                'SELECT F.RDB$FIELD_LENGTH AS "field_length" -- Is equals to char_length * bytes_per_char
                        FROM RDB$RELATION_FIELDS RF
                        JOIN RDB$FIELDS F ON RF.RDB$FIELD_SOURCE = F.RDB$FIELD_NAME
                        WHERE RF.RDB$RELATION_NAME = \'%s\' AND
                              RF.RDB$FIELD_NAME IN (%s);',
                $tablename,
                "'" . implode("', '", $columns) . "'"
            )
        );

        $keySize = 0;
        foreach ($rows as $row) {
            $colSize = $row['field_length'];

            // Calc segment length
            $keySize += (($colSize + 3) - ($colSize + 3) % 4) / 4 * 5;
        }

        // Calc key length
        $keySize = ($keySize + 3) - ($keySize + 3) % 4;

        return $keySize <= $pageSize;
    }


}
