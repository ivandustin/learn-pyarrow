import pyarrow as pa

def write(table):
    batches = table.to_batches()
    first   = batches[0]
    with pa.OSFile('pyarrow.dat', 'wb') as file:
        writer = pa.ipc.new_stream(file, first.schema)
        writer.write_table(table)

def read():
    with pa.memory_map('pyarrow.dat', 'r') as file:
        reader = pa.ipc.open_stream(file)
        table  = pa.Table.from_batches(reader)
    return table

def main():
    table = pa.Table.from_pydict({'a': [1, 2, 3]})
    write(table)
    table = read()
    print(table)
    print(table['a'])

main()
