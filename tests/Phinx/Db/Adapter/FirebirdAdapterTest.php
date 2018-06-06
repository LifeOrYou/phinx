<?php

namespace Test\Phinx\Db\Adapter;

use Phinx\Db\Adapter\AdapterInterface;
use Phinx\Db\Adapter\FirebirdAdapter;
use Phinx\Db\Adapter\MysqlAdapter;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Input\ArrayInput;
use Symfony\Component\Console\Input\InputDefinition;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\BufferedOutput;
use Symfony\Component\Console\Output\NullOutput;
use Symfony\Component\Console\Output\StreamOutput;

class FirebirdAdapterTest extends TestCase
{
    /**
     * @var \Phinx\Db\Adapter\FirebirdAdapter
     */
    private $adapter;

    public function setUp()
    {
        if (!TESTS_PHINX_DB_ADAPTER_FB_ENABLED) {
            $this->markTestSkipped('Firebird tests disabled. See TESTS_PHINX_DB_ADAPTER_FB_ENABLED constant.');
        }

        $options = [
            'host' => TESTS_PHINX_DB_ADAPTER_FB_HOST,
            'name' => TESTS_PHINX_DB_ADAPTER_FB_DATABASE,
            'user' => TESTS_PHINX_DB_ADAPTER_FB_USERNAME,
            'pass' => TESTS_PHINX_DB_ADAPTER_FB_PASSWORD,
            'port' => TESTS_PHINX_DB_ADAPTER_FB_PORT
        ];
        $this->adapter = new FirebirdAdapter($options, new ArrayInput([]), new NullOutput());

        // ensure the database is empty for each test
        $this->adapter->dropDatabase($options['name']);
        $this->adapter->createDatabase($options['name']);

        // leave the adapter in a disconnected state for each test
        $this->adapter->disconnect();
    }

    public function tearDown()
    {
        unset($this->adapter);
    }

    public function testConnection()
    {
        $this->assertInstanceOf('PDO', $this->adapter->getConnection());
    }

    public function testConnectionWithoutPort()
    {
        $options = $this->adapter->getOptions();
        unset($options['port']);
        $this->adapter->setOptions($options);
        $this->assertInstanceOf('PDO', $this->adapter->getConnection());
    }

    public function testConnectionWithInvalidCredentials()
    {
        $options = [
            'host' => TESTS_PHINX_DB_ADAPTER_FB_HOST,
            'name' => TESTS_PHINX_DB_ADAPTER_FB_DATABASE,
            'port' => TESTS_PHINX_DB_ADAPTER_FB_PORT,
            'user' => 'invaliduser',
            'pass' => 'invalidpass'
        ];

        try {
            $adapter = new FirebirdAdapter($options, new ArrayInput([]), new NullOutput());
            $adapter->connect();
            $this->fail('Expected the adapter to throw an exception');
        } catch (\InvalidArgumentException $e) {
            $this->assertInstanceOf(
                'InvalidArgumentException',
                $e,
                'Expected exception of type InvalidArgumentException, got ' . get_class($e)
            );
            $this->assertRegExp('/There was a problem connecting to the database/', $e->getMessage());
        }
    }

    public function testCreatingTheSchemaTableOnConnect()
    {
        $this->adapter->connect();
        $this->assertTrue($this->adapter->hasTable($this->adapter->getSchemaTableName()));
        $this->adapter->dropTable($this->adapter->getSchemaTableName());
        $this->assertFalse($this->adapter->hasTable($this->adapter->getSchemaTableName()));
        $this->adapter->disconnect();
        $this->adapter->connect();
        $this->assertTrue($this->adapter->hasTable($this->adapter->getSchemaTableName()));
    }

    public function testSchemaTableIsCreatedWithPrimaryKey()
    {
        $this->adapter->connect();
        $table = new \Phinx\Db\Table($this->adapter->getSchemaTableName(), [], $this->adapter);
        $this->assertTrue($this->adapter->hasIndex($this->adapter->getSchemaTableName(), ['version']));
    }

    public function testQuoteTableName()
    {
        $this->assertEquals('"test_table"', $this->adapter->quoteTableName('test_table'));
        $this->assertEquals('"test_table"."test_column"', $this->adapter->quoteTableName('test_table.test_column'));
    }

    public function testQuoteColumnName()
    {
        $this->assertEquals('"test_column"', $this->adapter->quoteColumnName('test_column'));
    }

    public function testCreateTable()
    {
        $table = new \Phinx\Db\Table('ntable', [], $this->adapter);
        $table->addColumn('realname', 'string')
              ->addColumn('email', 'integer')
              ->save();
        $this->assertTrue($this->adapter->hasTable('ntable'));
        $this->assertTrue($this->adapter->hasColumn('ntable', 'id'));
        $this->assertTrue($this->adapter->hasColumn('ntable', 'realname'));
        $this->assertTrue($this->adapter->hasColumn('ntable', 'email'));
        $this->assertFalse($this->adapter->hasColumn('ntable', 'address'));
    }

    public function testCreateTableWithComment()
    {
        $tableComment = 'Table comment';
        $table = new \Phinx\Db\Table('ntable', ['comment' => $tableComment], $this->adapter);
        $table->addColumn('realname', 'string')
              ->save();
        $this->assertTrue($this->adapter->hasTable('ntable'));
        $this->assertTrue($this->adapter->hasColumn('ntable', 'id'));
        $this->assertTrue($this->adapter->hasColumn('ntable', 'realname'));
        $this->assertFalse($this->adapter->hasColumn('ntable', 'address'));

        $rows = $this->adapter->fetchAll('SELECT rdb$description FROM rdb$relations WHERE rdb$relation_name=\'ntable\'');
        $comment = $rows[0];

        $this->assertEquals($tableComment, $comment[0], 'Dont set table comment correctly');
    }

    public function testCreateTableWithForeignKeys()
    {
        $tag_table = new \Phinx\Db\Table('ntable_tag', [], $this->adapter);
        $tag_table->addColumn('realname', 'string')
                  ->save();

        $table = new \Phinx\Db\Table('ntable', [], $this->adapter);
        $table->addColumn('realname', 'string')
              ->addColumn('tag_id', 'integer')
              ->addForeignKey('tag_id', 'ntable_tag', 'id', ['delete' => 'NO_ACTION', 'update' => 'NO_ACTION'])
              ->save();

        $this->assertTrue($this->adapter->hasTable('ntable'));
        $this->assertTrue($this->adapter->hasColumn('ntable', 'id'));
        $this->assertTrue($this->adapter->hasColumn('ntable', 'realname'));
        $this->assertFalse($this->adapter->hasColumn('ntable', 'address'));

        $rows = $this->adapter->fetchAll(sprintf(
            'SELECT
                        TRIM(rc.rdb$relation_name) as "table_name",
                        TRIM(dis.rdb$field_name) AS "column_name",
                        TRIM(mrc.rdb$relation_name) AS "referenced_table_name",
                        TRIM(mis.rdb$field_name) AS "referenced_column_name",
                        TRIM(drc.rdb$update_rule) AS "on_update",
                        TRIM(drc.rdb$delete_rule) AS "on_delete"
                    FROM
                        rdb$relation_constraints rc
                        JOIN rdb$index_segments dis ON rc.rdb$index_name = dis.rdb$index_name
                        JOIN rdb$ref_constraints drc ON drc.rdb$constraint_name = rc.rdb$constraint_name -- Master indeksas
                        JOIN rdb$relation_constraints mrc ON drc.rdb$const_name_uq = mrc.rdb$constraint_name
                        JOIN rdb$index_segments mis ON mrc.rdb$index_name = mis.rdb$index_name
                    WHERE
                        rc.rdb$constraint_type = \'FOREIGN KEY\'
                        AND rc.rdb$relation_name = \'ntable\'',
            TESTS_PHINX_DB_ADAPTER_FB_DATABASE
        ));
        $foreignKey = $rows[0];

        $this->assertEquals('ntable', $foreignKey['table_name']);
        $this->assertEquals('tag_id', $foreignKey['column_name']);
        $this->assertEquals('ntable_tag', $foreignKey['referenced_table_name']);
        $this->assertEquals('id', $foreignKey['referenced_column_name']);
        $this->assertEquals('NO ACTION', $foreignKey['on_update']);
        $this->assertEquals('NO ACTION', $foreignKey['on_delete']);
    }

    public function testCreateTableCustomIdColumn()
    {
        $table = new \Phinx\Db\Table('ntable', ['id' => 'custom_id'], $this->adapter);
        $table->addColumn('realname', 'string')
              ->addColumn('email', 'integer')
              ->save();

        $this->assertTrue($this->adapter->hasTable('ntable'));
        $this->assertTrue($this->adapter->hasColumn('ntable', 'custom_id'));
        $this->assertTrue($this->adapter->hasColumn('ntable', 'realname'));
        $this->assertTrue($this->adapter->hasColumn('ntable', 'email'));
        $this->assertFalse($this->adapter->hasColumn('ntable', 'address'));
    }

    public function testCreateTableWithNoPrimaryKey()
    {
        $options = [
            'id' => false
        ];
        $table = new \Phinx\Db\Table('atable', $options, $this->adapter);
        $table->addColumn('user_id', 'integer')
              ->save();

        $this->assertFalse($this->adapter->hasColumn('atable', 'id'));
    }

    public function testCreateTableWithMultiplePrimaryKeys()
    {
        $options = [
            'id' => false,
            'primary_key' => ['user_id', 'tag_id']
        ];
        $table = new \Phinx\Db\Table('table1', $options, $this->adapter);
        $table->addColumn('user_id', 'integer')
              ->addColumn('tag_id', 'integer')
              ->save();

        $this->assertTrue($this->adapter->hasIndex('table1', ['user_id', 'tag_id']));
        $this->assertTrue($this->adapter->hasIndex('table1', ['USER_ID', 'tag_id']));
        $this->assertFalse($this->adapter->hasIndex('table1', ['tag_id', 'user_email']));
    }

    public function testCreateTableWithMultipleIndexes()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->addColumn('email', 'string')
              ->addColumn('name', 'string')
              ->addIndex('email')
              ->addIndex('name')
              ->save();
        $this->assertTrue($this->adapter->hasIndex('table1', ['email']));
        $this->assertTrue($this->adapter->hasIndex('table1', ['name']));
        $this->assertFalse($this->adapter->hasIndex('table1', ['email', 'user_email']));
        $this->assertFalse($this->adapter->hasIndex('table1', ['email', 'user_name']));
    }

    public function testCreateTableWithUniqueIndexes()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->addColumn('email', 'string')
              ->addIndex('email', ['unique' => true])
              ->save();
        $this->assertTrue($this->adapter->hasIndex('table1', ['email']));
        $this->assertFalse($this->adapter->hasIndex('table1', ['email', 'user_email']));
    }

    public function testCreateTableWithNamedIndex()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->addColumn('email', 'string')
              ->addIndex('email', ['name' => 'myemailindex'])
              ->save();
        $this->assertTrue($this->adapter->hasIndex('table1', ['email']));
        $this->assertFalse($this->adapter->hasIndex('table1', ['email', 'user_email']));
        $this->assertTrue($this->adapter->hasIndexByName('table1', 'myemailindex'));
    }

    public function testCreateTableWithMultiplePKsAndUniqueIndexes()
    {
        $this->markTestIncomplete(); // ??
    }

    public function testCreateTableAndInheritDefaultCollation()
    {
        $options = [
            'host' => TESTS_PHINX_DB_ADAPTER_FB_HOST,
            'name' => TESTS_PHINX_DB_ADAPTER_FB_DATABASE,
            'user' => TESTS_PHINX_DB_ADAPTER_FB_USERNAME,
            'pass' => TESTS_PHINX_DB_ADAPTER_FB_PASSWORD,
            'port' => TESTS_PHINX_DB_ADAPTER_FB_PORT,
            'charset' => 'utf8',
            'collation' => 'unicode_ci',
        ];

        $adapter = new FirebirdAdapter($options, new ArrayInput([]), new NullOutput());

        // Ensure the database is empty and the adapter is in a disconnected state
        $adapter->dropDatabase($options['name']);
        $adapter->createDatabase($options['name']);
        $adapter->disconnect();

        $table = new \Phinx\Db\Table('table_with_default_collation', [], $adapter);
        $table->addColumn('name', 'string')
              ->save();
        $this->assertTrue($adapter->hasTable('table_with_default_collation'));
        $row = $adapter->fetchRow(sprintf(
            'SELECT LOWER(TRIM(coll.rdb$collation_name)) AS "field_collation"
                    FROM rdb$relation_fields r
                    LEFT JOIN rdb$fields f ON r.rdb$field_source = f.rdb$field_name
                    LEFT JOIN rdb$collations coll ON f.rdb$collation_id = coll.rdb$collation_id AND f.rdb$character_set_id = coll.rdb$character_set_id
                    WHERE r.rdb$relation_name = \'%s\' AND r.rdb$field_name = \'%s\'',
                'table_with_default_collation',
                'name'
            ));

        $this->assertEquals($options['collation'], $row['field_collation']);
    }

    public function testCreateTableWithLatin1Collate()
    {
        // In firebird latin-1 is iso8859_1 collation
        $table = new \Phinx\Db\Table('latin1_table', ['charset' => 'iso8859_1', 'collation' => 'iso8859_1'], $this->adapter);
        $table->addColumn('name', 'string')
              ->save();
        $this->assertTrue($this->adapter->hasTable('latin1_table'));
        $row = $this->adapter->fetchRow(sprintf(
            'SELECT LOWER(TRIM(coll.rdb$collation_name)) AS "field_collation"
                    FROM rdb$relation_fields r
                    LEFT JOIN rdb$fields f ON r.rdb$field_source = f.rdb$field_name
                    LEFT JOIN rdb$collations coll ON f.rdb$collation_id = coll.rdb$collation_id AND f.rdb$character_set_id = coll.rdb$character_set_id
                    WHERE r.rdb$relation_name = \'%s\' AND r.rdb$field_name = \'%s\'',
                'latin1_table',
            'name'
            ));
        $this->assertEquals('iso8859_1', $row['field_collation']);
    }

    public function testRenameTable()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->save();
        $this->assertTrue($this->adapter->hasTable('table1'));
        $this->assertFalse($this->adapter->hasTable('table2'));
        try {
            $this->adapter->renameTable('table1', 'table2');
            $this->fail('Expected the adapter to throw an exception');
        } catch (\Exception $e) {
            $this->assertInstanceOf(
                'Exception',
                $e,
                'Expected exception of type Exception, got ' . get_class($e)
            );
            $this->assertRegExp('/Firebird can\'t rename table/', $e->getMessage());
        }
        /*$this->assertFalse($this->adapter->hasTable('table1'));
        $this->assertTrue($this->adapter->hasTable('table2'));*/
    }

    public function testAddColumn()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->save();
        $this->assertFalse($table->hasColumn('email'));
        $table->addColumn('email', 'string')
              ->save();
        $this->assertTrue($table->hasColumn('email'));
        $table->addColumn('realname', 'string', ['after' => 'id'])
              ->save();
        $rows = $this->adapter->fetchAll('SELECT TRIM(rdb$field_name) AS "field_name"
                                                FROM rdb$relation_fields
                                                WHERE rdb$relation_name = \'table1\'
                                                ORDER BY rdb$field_position');
        $this->assertEquals('realname', $rows[1]['field_name']);
    }

    public function testAddColumnWithDefaultValue()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->save();
        $table->addColumn('default_zero', 'string', ['default' => 'test'])
              ->save();
        $rows = $this->adapter->fetchAll('SELECT rdb$default_source AS "field_default"
                                                FROM rdb$relation_fields
                                                WHERE rdb$relation_name = \'table1\' AND rdb$field_name = \'default_zero\'');
        $this->assertEquals('test', $this->adapter->parseDefault('string', $rows[0]['field_default']));
    }

    public function testAddColumnWithDefaultZero()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->save();
        $table->addColumn('default_zero', 'integer', ['default' => 0])
              ->save();
        $rows = $this->adapter->fetchAll('SELECT rdb$default_source AS "field_default"
                                                FROM rdb$relation_fields
                                                WHERE rdb$relation_name = \'table1\' AND rdb$field_name = \'default_zero\'');
        $this->assertNotNull($rows[0]['field_default']);
        $this->assertEquals('0', $this->adapter->parseDefault('integer', $rows[0]['field_default']));
    }

    public function testAddColumnWithDefaultEmptyString()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->save();
        $table->addColumn('default_empty', 'string', ['default' => ''])
              ->save();
        $rows = $this->adapter->fetchAll('SELECT rdb$default_source AS "field_default"
                                                FROM rdb$relation_fields
                                                WHERE rdb$relation_name = \'table1\' AND rdb$field_name = \'default_empty\'');
        $this->assertEquals('', $this->adapter->parseDefault('string', $rows[0]['field_default']));
    }

    public function testAddColumnWithDefaultBoolean()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->save();
        $table->addColumn('default_true', 'boolean', ['default' => true])
              ->addColumn('default_false', 'boolean', ['default' => false])
              ->save();
        $rows = $this->adapter->fetchAll('SELECT rdb$default_source AS "field_default"
                                                FROM rdb$relation_fields
                                                WHERE rdb$relation_name = \'table1\'');
        $this->assertEquals('TRUE', $this->adapter->parseDefault('boolean', $rows[1]['field_default']));
        $this->assertEquals('FALSE', $this->adapter->parseDefault('boolean', $rows[2]['field_default']));
    }

    public function testAddStringColumnWithCustomCollation()
    {
        $table = new \Phinx\Db\Table('table_custom_collation', ['collation' => 'utf8'], $this->adapter);
        $table->save();
        $this->assertFalse($table->hasColumn('string_collation_default'));
        $this->assertFalse($table->hasColumn('string_collation_custom'));
        $table->addColumn('string_collation_default', 'string', [])->save();
        $table->addColumn('string_collation_custom', 'string', ['collation' => 'unicode_ci'])->save();
        $rows = $this->adapter->fetchAll('SELECT LOWER(TRIM(coll.rdb$collation_name)) AS "field_collation"
                    FROM rdb$relation_fields r
                    LEFT JOIN rdb$fields f ON r.rdb$field_source = f.rdb$field_name
                    LEFT JOIN rdb$collations coll ON f.rdb$collation_id = coll.rdb$collation_id AND f.rdb$character_set_id = coll.rdb$character_set_id
                    WHERE r.rdb$relation_name = \'table_custom_collation\'');
        $this->assertEquals('utf8', $rows[1]['field_collation']);
        $this->assertEquals('unicode_ci', $rows[2]['field_collation']);
    }

    public function testRenameColumn()
    {
        $table = new \Phinx\Db\Table('t', [], $this->adapter);
        $table->addColumn('column1', 'string')
              ->save();
        $this->assertTrue($this->adapter->hasColumn('t', 'column1'));
        $this->assertFalse($this->adapter->hasColumn('t', 'column2'));
        $this->adapter->renameColumn('t', 'column1', 'column2');
        $this->assertFalse($this->adapter->hasColumn('t', 'column1'));
        $this->assertTrue($this->adapter->hasColumn('t', 'column2'));
    }

    public function testRenameColumnIsCaseSensitive()
    {
        $table = new \Phinx\Db\Table('t', [], $this->adapter);
        $table->addColumn('columnOne', 'string')
            ->save();
        $this->assertTrue($this->adapter->hasColumn('t', 'columnOne'));
        $this->assertFalse($this->adapter->hasColumn('t', 'columnTwo'));
        $this->adapter->renameColumn('t', 'columnOne', 'columnTwo');
        $this->assertFalse($this->adapter->hasColumn('t', 'columnOne'));
        $this->assertTrue($this->adapter->hasColumn('t', 'columnTwo'));
    }

    public function testRenamingANonExistentColumn()
    {
        $table = new \Phinx\Db\Table('t', [], $this->adapter);
        $table->addColumn('column1', 'string')
              ->save();

        try {
            $this->adapter->renameColumn('t', 'column2', 'column1');
            $this->fail('Expected the adapter to throw an exception');
        } catch (\InvalidArgumentException $e) {
            $this->assertInstanceOf(
                'InvalidArgumentException',
                $e,
                'Expected exception of type InvalidArgumentException, got ' . get_class($e)
            );
            $this->assertEquals('The specified column does not exist: column2', $e->getMessage());
        }
    }

    public function testChangeColumn()
    {
        $table = new \Phinx\Db\Table('t', [], $this->adapter);
        $table->addColumn('column1', 'string')
              ->save();
        $this->assertTrue($this->adapter->hasColumn('t', 'column1'));
        $newColumn1 = new \Phinx\Db\Table\Column();
        $newColumn1->setType('string');
        $table->changeColumn('column1', $newColumn1);
        $this->assertTrue($this->adapter->hasColumn('t', 'column1'));
        $newColumn2 = new \Phinx\Db\Table\Column();
        $newColumn2->setName('column2')
                   ->setType('string');
        $table->changeColumn('column1', $newColumn2);
        $this->assertFalse($this->adapter->hasColumn('t', 'column1'));
        $this->assertTrue($this->adapter->hasColumn('t', 'column2'));
    }

    public function testChangeColumnDefaultValue()
    {
        $table = new \Phinx\Db\Table('t', [], $this->adapter);
        $table->addColumn('column1', 'string', ['default' => 'test'])
              ->save();
        $newColumn1 = new \Phinx\Db\Table\Column();
        $newColumn1->setDefault('test1')
                   ->setType('string');
        $table->changeColumn('column1', $newColumn1);
        $rows = $this->adapter->fetchAll('SELECT rdb$default_source AS "field_default"
                                                FROM rdb$relation_fields
                                                WHERE rdb$relation_name = \'t\' AND rdb$field_name = \'column1\'');
        $this->assertNotNull($rows[0]['field_default']);
        $this->assertEquals('test1', $this->adapter->parseDefault('string', $rows[0]['field_default']));
    }

    public function testChangeColumnDefaultToZero()
    {
        $table = new \Phinx\Db\Table('t', [], $this->adapter);
        $table->addColumn('column1', 'integer')
              ->save();
        $newColumn1 = new \Phinx\Db\Table\Column();
        $newColumn1->setDefault(0)
                   ->setType('integer');
        $table->changeColumn('column1', $newColumn1);
        $rows = $this->adapter->fetchAll('SELECT rdb$default_source AS "field_default"
                                                FROM rdb$relation_fields
                                                WHERE rdb$relation_name = \'t\' AND rdb$field_name = \'column1\'');
        $this->assertNotNull($rows[0]['field_default']);
        $this->assertEquals("0", $this->adapter->parseDefault('integer', $rows[0]['field_default']));
    }

    public function testChangeColumnDefaultToNull()
    {
        $table = new \Phinx\Db\Table('t', [], $this->adapter);
        $table->addColumn('column1', 'string', ['default' => 'test'])
              ->save();
        $newColumn1 = new \Phinx\Db\Table\Column();
        $newColumn1->setDefault(null)
                   ->setType('string');
        $table->changeColumn('column1', $newColumn1);
        $rows = $this->adapter->fetchAll('SELECT rdb$default_source AS "field_default"
                                                FROM rdb$relation_fields
                                                WHERE rdb$relation_name = \'t\' AND rdb$field_name = \'column1\'');
        $this->assertNull($rows[0]['field_default']);
    }

    public function testDropColumn()
    {
        $table = new \Phinx\Db\Table('t', [], $this->adapter);
        $table->addColumn('column1', 'string')
              ->save();
        $this->assertTrue($this->adapter->hasColumn('t', 'column1'));
        $this->adapter->dropColumn('t', 'column1');
        $this->assertFalse($this->adapter->hasColumn('t', 'column1'));
    }

    public function testGetColumns()
    {
        $table = new \Phinx\Db\Table('t', [], $this->adapter);
        $table->addColumn('column1', 'string')
            ->addColumn('column2', 'integer', ['limit' => FirebirdAdapter::INT_SMALL])
            ->addColumn('column3', 'integer')
            ->addColumn('column4', 'biginteger')
            ->addColumn('column5', 'text')
            ->addColumn('column6', 'float')
            ->addColumn('column7', 'decimal')
            ->addColumn('column8', 'time')
            ->addColumn('column9', 'timestamp')
            ->addColumn('column10', 'date')
            ->addColumn('column11', 'boolean')
            ->addColumn('column12', 'datetime')
            ->addColumn('column13', 'binary')
            ->addColumn('column14', 'string', ['limit' => 10]);
        $pendingColumns = $table->getPendingColumns();
        $table->save();
        $columns = $this->adapter->getColumns('t');
        $this->assertCount(count($pendingColumns) + 1, $columns);
        for ($i = 0; $i++; $i < count($pendingColumns)) {
            $this->assertEquals($pendingColumns[$i], $columns[$i + 1]);
        }
    }

    public function testAddIndex()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->addColumn('email', 'string')
              ->save();
        $this->assertFalse($table->hasIndex('email'));
        $table->addIndex('email')
              ->save();
        $this->assertTrue($table->hasIndex('email'));
    }

    public function testDropIndex()
    {
        // single column index
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->addColumn('email', 'string')
              ->addIndex('email')
              ->save();
        $this->assertTrue($table->hasIndex('email'));
        $this->adapter->dropIndex($table->getName(), 'email');
        $this->assertFalse($table->hasIndex('email'));

        // multiple column index
        $table2 = new \Phinx\Db\Table('table2', [], $this->adapter);
        $table2->addColumn('fname', 'string')
               ->addColumn('lname', 'string')
               ->addIndex(['fname', 'lname'])
               ->save();
        $this->assertTrue($table2->hasIndex(['fname', 'lname']));
        $this->adapter->dropIndex($table2->getName(), ['fname', 'lname']);
        $this->assertFalse($table2->hasIndex(['fname', 'lname']));

        // index with name specified, but dropping it by column name
        $table3 = new \Phinx\Db\Table('table3', [], $this->adapter);
        $table3->addColumn('email', 'string')
              ->addIndex('email', ['name' => 'someindexname'])
              ->save();
        $this->assertTrue($table3->hasIndex('email'));
        $this->adapter->dropIndex($table3->getName(), 'email');
        $this->assertFalse($table3->hasIndex('email'));

        // multiple column index with name specified
        $table4 = new \Phinx\Db\Table('table4', [], $this->adapter);
        $table4->addColumn('fname', 'string')
               ->addColumn('lname', 'string')
               ->addIndex(['fname', 'lname'], ['name' => 'multiname'])
               ->save();
        $this->assertTrue($table4->hasIndex(['fname', 'lname']));
        $this->adapter->dropIndex($table4->getName(), ['fname', 'lname']);
        $this->assertFalse($table4->hasIndex(['fname', 'lname']));

        // don't drop multiple column index when dropping single column
        $table2 = new \Phinx\Db\Table('table5', [], $this->adapter);
        $table2->addColumn('fname', 'string')
               ->addColumn('lname', 'string')
               ->addIndex(['fname', 'lname'])
               ->save();
        $this->assertTrue($table2->hasIndex(['fname', 'lname']));
        $this->adapter->dropIndex($table2->getName(), ['fname']);
        $this->assertTrue($table2->hasIndex(['fname', 'lname']));

        // don't drop multiple column index with name specified when dropping
        // single column
        $table4 = new \Phinx\Db\Table('table6', [], $this->adapter);
        $table4->addColumn('fname', 'string')
               ->addColumn('lname', 'string')
               ->addIndex(['fname', 'lname'], ['name' => 'multiname'])
               ->save();
        $this->assertTrue($table4->hasIndex(['fname', 'lname']));
        $this->adapter->dropIndex($table4->getName(), ['fname']);
        $this->assertTrue($table4->hasIndex(['fname', 'lname']));
    }

    public function testDropIndexByName()
    {
        // single column index
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->addColumn('email', 'string')
              ->addIndex('email', ['name' => 'myemailindex'])
              ->save();
        $this->assertTrue($table->hasIndex('email'));
        $this->adapter->dropIndexByName($table->getName(), 'myemailindex');
        $this->assertFalse($table->hasIndex('email'));

        // multiple column index
        $table2 = new \Phinx\Db\Table('table2', [], $this->adapter);
        $table2->addColumn('fname', 'string')
               ->addColumn('lname', 'string')
               ->addIndex(['fname', 'lname'], ['name' => 'twocolumnindex'])
               ->save();
        $this->assertTrue($table2->hasIndex(['fname', 'lname']));
        $this->adapter->dropIndexByName($table2->getName(), 'twocolumnindex');
        $this->assertFalse($table2->hasIndex(['fname', 'lname']));
    }

    public function testCreateIndexTooLong() {
        // muiltiple column index
        try {
            $table2 = new \Phinx\Db\Table('table2', [], $this->adapter);
            $table2->addColumn('fname', 'string', ['limit' => 500])
                ->addColumn('lname', 'string', ['limit' => 500])
                ->addIndex(['fname', 'lname'], ['name' => 'twocolumnindex'])
                ->save();
        } catch (\Exception $e) {
            $this->assertInstanceOf(
                'Exception',
                $e,
                'Expected exception of type Exception, got ' . get_class($e)
            );
            $this->assertRegExp('/Firebird index segment is too long/', $e->getMessage());
        }

        $this->assertFalse($this->adapter->hasIndexByName('table1', 'twocolumnindex'));
    }

    public function testAddForeignKey()
    {
        $refTable = new \Phinx\Db\Table('ref_table', [], $this->adapter);
        $refTable->addColumn('field1', 'string')->save();

        $table = new \Phinx\Db\Table('table', [], $this->adapter);
        $table->addColumn('ref_table_id', 'integer')->save();

        $fk = new \Phinx\Db\Table\ForeignKey();
        $fk->setReferencedTable($refTable)
           ->setColumns(['ref_table_id'])
           ->setReferencedColumns(['id']);

        $this->adapter->addForeignKey($table, $fk);
        $this->assertTrue($this->adapter->hasForeignKey($table->getName(), ['ref_table_id']));
    }

    public function testAddForeignKeyRestrict() {
        $this->markTestIncomplete();
    }

    public function testAddForeignKeyForTableWithUnsignedPK()
    {
        $refTable = new \Phinx\Db\Table('ref_table', ['signed' => false], $this->adapter);
        $refTable->addColumn('field1', 'string')->save();

        $table = new \Phinx\Db\Table('table', [], $this->adapter);
        $table->addColumn('ref_table_id', 'integer', ['signed' => false])->save();

        $fk = new \Phinx\Db\Table\ForeignKey();
        $fk->setReferencedTable($refTable)
           ->setColumns(['ref_table_id'])
           ->setReferencedColumns(['id']);

        $this->adapter->addForeignKey($table, $fk);
        $this->assertTrue($this->adapter->hasForeignKey($table->getName(), ['ref_table_id']));
    }

    public function testDropForeignKey()
    {
        $refTable = new \Phinx\Db\Table('ref_table', [], $this->adapter);
        $refTable->addColumn('field1', 'string')->save();

        $table = new \Phinx\Db\Table('table', [], $this->adapter);
        $table->addColumn('ref_table_id', 'integer')->save();

        $fk = new \Phinx\Db\Table\ForeignKey();
        $fk->setReferencedTable($refTable)
           ->setColumns(['ref_table_id'])
           ->setReferencedColumns(['id']);

        $this->adapter->addForeignKey($table, $fk);
        $this->adapter->dropForeignKey($table->getName(), ['ref_table_id']);
        $this->assertFalse($this->adapter->hasForeignKey($table->getName(), ['ref_table_id']));
    }

    public function testDropForeignKeyForTableWithUnsignedPK()
    {
        $refTable = new \Phinx\Db\Table('ref_table', ['signed' => false], $this->adapter);
        $refTable->addColumn('field1', 'string')->save();

        $table = new \Phinx\Db\Table('table', [], $this->adapter);
        $table->addColumn('ref_table_id', 'integer', ['signed' => false])->save();

        $fk = new \Phinx\Db\Table\ForeignKey();
        $fk->setReferencedTable($refTable)
           ->setColumns(['ref_table_id'])
           ->setReferencedColumns(['id']);

        $this->adapter->addForeignKey($table, $fk);
        $this->adapter->dropForeignKey($table->getName(), ['ref_table_id']);
        $this->assertFalse($this->adapter->hasForeignKey($table->getName(), ['ref_table_id']));
    }

    public function testDropForeignKeyAsString()
    {
        $refTable = new \Phinx\Db\Table('ref_table', [], $this->adapter);
        $refTable->addColumn('field1', 'string')->save();

        $table = new \Phinx\Db\Table('table', [], $this->adapter);
        $table->addColumn('ref_table_id', 'integer')->save();

        $fk = new \Phinx\Db\Table\ForeignKey();
        $fk->setReferencedTable($refTable)
           ->setColumns(['ref_table_id'])
           ->setReferencedColumns(['id']);

        $this->adapter->addForeignKey($table, $fk);
        $this->adapter->dropForeignKey($table->getName(), 'ref_table_id');
        $this->assertFalse($this->adapter->hasForeignKey($table->getName(), ['ref_table_id']));
    }

    public function testHasForeignKey()
    {
        $refTable = new \Phinx\Db\Table('ref_table', [], $this->adapter);
        $refTable->addColumn('field1', 'string')->save();

        $table = new \Phinx\Db\Table('table', [], $this->adapter);
        $table->addColumn('ref_table_id', 'integer')->save();

        $fk = new \Phinx\Db\Table\ForeignKey();
        $fk->setReferencedTable($refTable)
           ->setColumns(['ref_table_id'])
           ->setReferencedColumns(['id']);

        $this->adapter->addForeignKey($table, $fk);
        $this->assertTrue($this->adapter->hasForeignKey($table->getName(), ['ref_table_id']));
        $this->assertFalse($this->adapter->hasForeignKey($table->getName(), ['ref_table_id2']));
    }

    public function testHasForeignKeyForTableWithUnsignedPK()
    {
        $refTable = new \Phinx\Db\Table('ref_table', ['signed' => false], $this->adapter);
        $refTable->addColumn('field1', 'string')->save();

        $table = new \Phinx\Db\Table('table', [], $this->adapter);
        $table->addColumn('ref_table_id', 'integer', ['signed' => false])->save();

        $fk = new \Phinx\Db\Table\ForeignKey();
        $fk->setReferencedTable($refTable)
           ->setColumns(['ref_table_id'])
           ->setReferencedColumns(['id']);

        $this->adapter->addForeignKey($table, $fk);
        $this->assertTrue($this->adapter->hasForeignKey($table->getName(), ['ref_table_id']));
        $this->assertFalse($this->adapter->hasForeignKey($table->getName(), ['ref_table_id2']));
    }

    public function testHasForeignKeyAsString()
    {
        $refTable = new \Phinx\Db\Table('ref_table', [], $this->adapter);
        $refTable->addColumn('field1', 'string')->save();

        $table = new \Phinx\Db\Table('table', [], $this->adapter);
        $table->addColumn('ref_table_id', 'integer')->save();

        $fk = new \Phinx\Db\Table\ForeignKey();
        $fk->setReferencedTable($refTable)
           ->setColumns(['ref_table_id'])
           ->setReferencedColumns(['id']);

        $this->adapter->addForeignKey($table, $fk);
        $this->assertTrue($this->adapter->hasForeignKey($table->getName(), 'ref_table_id'));
        $this->assertFalse($this->adapter->hasForeignKey($table->getName(), 'ref_table_id2'));
    }

    public function testHasForeignKeyWithConstraint()
    {
        $refTable = new \Phinx\Db\Table('ref_table', [], $this->adapter);
        $refTable->addColumn('field1', 'string')->save();

        $table = new \Phinx\Db\Table('table', [], $this->adapter);
        $table->addColumn('ref_table_id', 'integer')->save();

        $fk = new \Phinx\Db\Table\ForeignKey();
        $fk->setReferencedTable($refTable)
           ->setColumns(['ref_table_id'])
           ->setConstraint("my_constraint")
           ->setReferencedColumns(['id']);

        $this->adapter->addForeignKey($table, $fk);
        $this->assertTrue($this->adapter->hasForeignKey($table->getName(), ['ref_table_id'], 'my_constraint'));
        $this->assertFalse($this->adapter->hasForeignKey($table->getName(), ['ref_table_id'], 'my_constraint2'));
    }

    public function testHasForeignKeyWithConstraintForTableWithUnsignedPK()
    {
        $refTable = new \Phinx\Db\Table('ref_table', ['signed' => false], $this->adapter);
        $refTable->addColumn('field1', 'string')->save();

        $table = new \Phinx\Db\Table('table', [], $this->adapter);
        $table->addColumn('ref_table_id', 'integer', ['signed' => false])->save();

        $fk = new \Phinx\Db\Table\ForeignKey();
        $fk->setReferencedTable($refTable)
           ->setColumns(['ref_table_id'])
           ->setConstraint("my_constraint")
           ->setReferencedColumns(['id']);

        $this->adapter->addForeignKey($table, $fk);
        $this->assertTrue($this->adapter->hasForeignKey($table->getName(), ['ref_table_id'], 'my_constraint'));
        $this->assertFalse($this->adapter->hasForeignKey($table->getName(), ['ref_table_id'], 'my_constraint2'));
    }

    public function testHasDatabase()
    {
        $this->assertFalse($this->adapter->hasDatabase('fake_database_name'));
        $this->assertTrue($this->adapter->hasDatabase(TESTS_PHINX_DB_ADAPTER_FB_DATABASE));
    }

    public function testDropDatabase()
    {
        $this->assertFalse($this->adapter->hasDatabase('phinx_temp_database'));
        $this->adapter->createDatabase('phinx_temp_database');
        $this->assertTrue($this->adapter->hasDatabase('phinx_temp_database'));
        $this->adapter->dropDatabase('phinx_temp_database');
        $this->assertFalse($this->adapter->hasDatabase('phinx_temp_database'));
    }

    public function testAddColumnWithComment()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->addColumn('column1', 'string', ['comment' => $comment = 'Comments from "column1"'])
              ->save();

        $rows = $this->adapter->fetchAll(sprintf(
            'SELECT rdb$description FROM rdb$relation_fields WHERE rdb$relation_name=\'%s\' AND rdb$field_name = \'%s\'',
            'table1',
            'column1'
        ));
        $columnWithComment = $rows[0];

        $this->assertEquals($comment, $columnWithComment['RDB$DESCRIPTION'], 'Dont set column comment correctly');
    }

    public function testHasColumn()
    {
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->addColumn('column1', 'string')
              ->save();

        $this->assertFalse($table->hasColumn('column2'));
        $this->assertTrue($table->hasColumn('column1'));
    }

    public function testHasColumnReservedName()
    {
        $tableQuoted = new \Phinx\Db\Table('group', [], $this->adapter);
        $tableQuoted->addColumn('value', 'string')
                    ->save();

        $this->assertFalse($tableQuoted->hasColumn('column2'));
        $this->assertTrue($tableQuoted->hasColumn('value'));
    }

    public function testBulkInsertData()
    {
        $data = [
            [
                'column1' => 'value1',
                'column2' => 1,
            ],
            [
                'column1' => 'value2',
                'column2' => 2,
            ],
            [
                'column1' => 'value3',
                'column2' => 3,
            ]
        ];
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->addColumn('column1', 'string')
            ->addColumn('column2', 'integer')
            ->addColumn('column3', 'string', ['default' => 'test'])
            ->insert($data);
        $this->adapter->createTable($table);
        $this->adapter->bulkinsert($table, $table->getData());
        $table->reset();

        $rows = $this->adapter->fetchAll('SELECT * FROM "table1"');
        $this->assertEquals('value1', $rows[0]['column1']);
        $this->assertEquals('value2', $rows[1]['column1']);
        $this->assertEquals('value3', $rows[2]['column1']);
        $this->assertEquals(1, $rows[0]['column2']);
        $this->assertEquals(2, $rows[1]['column2']);
        $this->assertEquals(3, $rows[2]['column2']);
        $this->assertEquals('test', $rows[0]['column3']);
        $this->assertEquals('test', $rows[2]['column3']);
    }

    public function testInsertData()
    {
        $data = [
            [
                'column1' => 'value1',
                'column2' => 1,
            ],
            [
                'column1' => 'value2',
                'column2' => 2,
            ],
            [
                'column1' => 'value3',
                'column2' => 3,
                'column3' => 'foo',
            ]
        ];
        $table = new \Phinx\Db\Table('table1', [], $this->adapter);
        $table->addColumn('column1', 'string')
            ->addColumn('column2', 'integer')
            ->addColumn('column3', 'string', ['default' => 'test'])
            ->insert($data)
            ->save();

        $rows = $this->adapter->fetchAll(sprintf('SELECT * FROM %s', $this->adapter->quoteTableName('table1')));
        $this->assertEquals('value1', $rows[0]['column1']);
        $this->assertEquals('value2', $rows[1]['column1']);
        $this->assertEquals('value3', $rows[2]['column1']);
        $this->assertEquals(1, $rows[0]['column2']);
        $this->assertEquals(2, $rows[1]['column2']);
        $this->assertEquals(3, $rows[2]['column2']);
        $this->assertEquals('test', $rows[0]['column3']);
        $this->assertEquals('foo', $rows[2]['column3']);
    }

    public function testDumpCreateTable()
    {
        $inputDefinition = new InputDefinition([new InputOption('dry-run')]);
        $this->adapter->setInput(new ArrayInput(['--dry-run' => true], $inputDefinition));

        $consoleOutput = new BufferedOutput();
        $this->adapter->setOutput($consoleOutput);

        $table = new \Phinx\Db\Table('table1', [], $this->adapter);

        $table->addColumn('column1', 'string')
            ->addColumn('column2', 'integer')
            ->addColumn('column3', 'string', ['default' => 'test'])
            ->save();

        $expectedOutput = <<<'OUTPUT'
CREATE TABLE "table1" ("id" INT GENERATED BY DEFAULT AS IDENTITY NOT NULL, "column1" VARCHAR(255) NOT NULL, "column2" INT NOT NULL, "column3" VARCHAR(255) NOT NULL DEFAULT 'test', PRIMARY KEY (`id`));
OUTPUT;
        $actualOutput = $consoleOutput->fetch();
        $this->assertContains($expectedOutput, $actualOutput, 'Passing the --dry-run option does not dump create table query to the output');
    }

    public function testGetPageSize() {
        // Test default page size
        $this->assertFalse($this->adapter->hasDatabase('phinx_testing_pagesize'));
        $this->adapter->createDatabase('phinx_testing_pagesize');
        $this->assertTrue($this->adapter->hasDatabase('phinx_testing_pagesize'));

        $this->assertEquals(8192, $this->adapter->getPageSize('phinx_testing_pagesize')); // 8192 is default pageSize
        $this->adapter->disconnect();
        $this->adapter->dropDatabase('phinx_testing_pagesize');
        $this->assertFalse($this->adapter->hasDatabase('phinx_testing_pagesize'));

        // Test custom page size
        $this->adapter->createDatabase('phinx_testing_pagesize', ['page_size' => 16384]);
        $this->assertTrue($this->adapter->hasDatabase('phinx_testing_pagesize'));

        $this->assertEquals(16384, $this->adapter->getPageSize('phinx_testing_pagesize'));
        $this->adapter->disconnect();
        $this->adapter->dropDatabase('phinx_testing_pagesize');
        $this->assertFalse($this->adapter->hasDatabase('phinx_testing_pagesize'));

        // Test invalid page size (Firebird takes the first lowest valid value)
        $this->adapter->createDatabase('phinx_testing_pagesize', ['page_size' => 10000]);
        $this->assertTrue($this->adapter->hasDatabase('phinx_testing_pagesize'));

        $this->assertEquals(8192, $this->adapter->getPageSize('phinx_testing_pagesize'));
        $this->adapter->disconnect();
        $this->adapter->dropDatabase('phinx_testing_pagesize');
        $this->assertFalse($this->adapter->hasDatabase('phinx_testing_pagesize'));
    }
}
