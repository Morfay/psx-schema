<?php

namespace PSX\Schema\Parser;

use PSX\Json\Parser;
use PSX\Schema\ParserInterface;
use PSX\Schema\PropertyType;
use PSX\Schema\Schema;
use RuntimeException;

/**
 * AvroSchema parser
 *
 * @author  Alexander Polozok <alexander@polozok.com>
 */
class AvroSchema implements ParserInterface
{
    public function parse($schema)
    {
        $data = Parser::decode($schema, true);
        $property = $this->parseAvroSchema($data);

        return new Schema($property);
    }

    private function parseAvroSchema($data)
    {
        $property    = new PropertyType();
        $property->setType('object');
        $property->setClass($data['name']);

        $required = [];
        foreach ($data['fields'] as $field) {
            $prop = new PropertyType();
            $prop->setType($field['type']);

            if (!array_key_exists('default', $field)) {
                $required[] = $field['name'];
            } else {
                $prop->setDefault($field['default']);
            }

            $property->addProperty($field['name'], $prop);
        }
        $property->setRequired($required);
        return $property;
    }

    public static function fromFile($file)
    {
        if (!empty($file) && is_file($file)) {
            $basePath = pathinfo($file, PATHINFO_DIRNAME);
            $parser   = new self($basePath);

            return $parser->parse(file_get_contents($file));
        } else {
            throw new RuntimeException('Could not load avro schema ' . $file);
        }
    }
}
