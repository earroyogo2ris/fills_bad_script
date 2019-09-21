#!/home/postmaster/venv/whatsapp/bin/python

import psycopg2 as infrastructure_db

import psycopg2.extras


db_cred = {'dbname': 'infrastructure', 'user': 'internalbot', 'password': 'enter the password here', 'host': 'nexus.go2ris.com'}
locations = {'savigneux': 5766, 'murserigne': 5765, 'hurumilly': 5764}

def run_this():
    bad_fills = get_unprocessed_bad_fills()
    add_bad_fills_to_fills_table(bad_fills)

def get_unprocessed_bad_fills():
    command = "select * from fills_bad where retailer_str like 'system%' and uid not like 'PROCESSED%'"
    return connect_infrastructure_db(command) 


def add_bad_fills_to_fills_table(bad_fills):
    for bad_fill in bad_fills:
        # Prepend 'PROCESSED' to uid 
        uid = '\'PROCESSED: %s\'' %(bad_fill['uid'])
        process_bad_fill_command = '''UPDATE fills_bad SET uid = %s WHERE id = %i''' %(uid, bad_fill['id'])
        connect_infrastructure_db(process_bad_fill_command, fetch=False)
        
        # Insert bad fill into the fills table
        insert_bad_fill_command = '''INSERT INTO 
                    fills(
                        retailer_id, 
                        location_id,
                        operator,
                        cartridge_id,
                        raw_name,
                        timestamp,
                        uid,
                        created_at,
                        updated_at,
                        cart_cartridge_id)
                    VALUES (%i,%i,'%s',%i,'%s','%s','%s','%s','%s',%i)''' %(
                        159,
                        int(locations[bad_fill['location_str']]),
                        bad_fill['operator'],
                        int(bad_fill['cartridge_id']),
                        bad_fill['raw_name'],
                        bad_fill['timestamp'],
                        bad_fill['uid'],
                        bad_fill['created_at'],
                        bad_fill['updated_at'],
                        int(bad_fill['cart_cartridge_id']))

        connect_infrastructure_db(insert_bad_fill_command, fetch=False)


def connect_infrastructure_db(command, fetch=True):
    connection_string = "dbname='%s' user='%s' host='%s' password=%s" %(db_cred['dbname'], db_cred['user'], db_cred['host'], db_cred['password'])
    with infrastructure_db.connect(connection_string) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(command)
            if fetch:
                return cur.fetchall()

if __name__ == "__main__":
    run_this()




# /home/postmaster/venv/whatsapp/bin/python /home/postmaster/Documents/fills_bad_migrator.py 

# Append this in the crontab -e
# # run the fills bad report
# 0 3 * * * 
