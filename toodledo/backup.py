"""
Backs up ToodleDo 
"""
import sys
import os
import requests
import yaml
import pandas as pd
from getpass import getpass
from requests_oauthlib import OAuth2Session
import requests
import urllib
import json
import logging

# TODO modify redirection URI? Localhost is a bit weird, there might be something running there.
# So, just play around with possibilities and see what works.
# TODO create a dummy user account and try to restore info there
# TODO Add writing scope
# TODO Commons with constants? Make sure the script is runnable form anywhere

CUR_FILE_DIR = os.path.dirname(os.path.realpath(__file__))+os.path.sep
API_URL_PREFIX = "http://api.toodledo.com/3/"
GET_URL_POSTFIX = '/get.php'

# Tasks: http://api.toodledo.com/3/tasks/index.php
DEFAULT_TASK_FIELDS = ["id", "title", "modified", "completed"]
OPTIONAL_TASK_FIELDS = ["folder", "context", "goal", "location", "tag", "startdate", "duedate",
        "duedatemod", "starttime", "duetime", "remind", "repeat", "status", "star", "priority",
        "length", "timer", "added", "note", "parent", "children", "order", "meta", "previous",
        "attachment", "shared", "addedby", "via", "attachments"]

DEFAULT_FOLDER_FIELDS = ["id","name","private","archived","ord"]
DEFAULT_CONTEXT_FIELDS = ["id","name","private"]
DEFAULT_GOAL_FIELDS = ["id","name","level","archived","contributes","note"]
DEFAULT_LOCATION_FIELDS = ["id","name","description","lat","lon"]
DEFAULT_NOTES_FIELDS = ["id","title","modified","added","folder","private","text"]
LIST_ROW_DEFAULT_FIELDS=["id","added","modified","version","list","cells"]
LIST_COL_DEFAULT_FIELDS=["id","title","type","sort","width"]


AUTHORIZATION_URL = "https://api.toodledo.com/3/account/authorize.php"
TOKEN_URL = 'https://api.toodledo.com/3/account/token.php'
TOKEN_FILENAME = CUR_FILE_DIR+"token.txt"
CONFIG_FILENAME = CUR_FILE_DIR+"config.yaml"
CLIENT_ID_FIELD = 'CLIENT_ID'
CLIENT_SECRET_FIELD = 'CLIENT_SECRET'
REDIRECT_URL_FIELD = 'REDIRECT_URL'
BACKUP_FOLDER_FIELD = 'BACKUP_FOLDER'
ALL_SCOPES = ["basic","folders", "tasks","notes","outlines","lists"]


def get_token_response(request_body):
    access_token, refresh_token = None, None
    token_response = requests.post(TOKEN_URL, data = request_body)
    if token_response.status_code == 200:
        token_dict = json.loads(token_response.text)
        if "access_token" in token_dict:
            access_token = token_dict["access_token"]
        if "refresh_token" in token_dict:
            refresh_token = token_dict["refresh_token"]
    else:
        logging.warning("Failed to refresh. Status: %d. Result:\n%s",
                token_response.status_code, str(token_response.text))
    return access_token, refresh_token


def get_authorization_response(config, oauth):
    authorization_url, state = oauth.authorization_url(AUTHORIZATION_URL)
    # Here print is intended. We are working with console.
    print('Please go to thir URL and authorize access:')
    print(authorization_url)
    authorization_response = input('Enter the full callback URL: ')
    return authorization_response


def refresh_tokens(config, access_token, refresh_token):
    # If failed to refresh, we'll be OK anyway
    body = {'client_id': config[CLIENT_ID_FIELD],
            'client_secret': config[CLIENT_SECRET_FIELD],
            'redirect_uri': config[REDIRECT_URL_FIELD],
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
    }
    try:
        new_access_token, new_refresh_token = get_token_response(body)
        if new_access_token is None:
            new_access_token = access_token
            logging.info("Keeping old access token: %s", new_access_token)
        else:
            logging.info("New access token: %s", new_access_token)

        if new_refresh_token is None:
            new_refresh_token = refresh_token
            logging.info("Keeping old refresh token: %s", new_refresh_token)
        else:
            logging.info("New refresh token: %s", new_refresh_token)
    except Exception as e:
        logging.warning("Failed to refresh. Might still be OK with old token.", str(e))
        new_access_token, new_refresh_token = access_token, refresh_token
    return new_access_token, new_refresh_token


def get_tokens_from_scratch(config):
    oauth = OAuth2Session(config[CLIENT_ID_FIELD],
            redirect_uri=config[REDIRECT_URL_FIELD],
            scope=ALL_SCOPES)
    authorization_response = get_authorization_response(config, oauth)

    connection_success=False
    first_time = True
    while not connection_success:
        try:
            if not first_time:
                logging.info("Trying to reconnect...")
                authorization_response = get_authorization_response(config, oauth)
            first_time = False
            code = urllib.parse.parse_qs(
                urllib.parse.urlsplit(authorization_response).query
            )["code"][0]
            # Just could not get in OAuth. It kept throwing
            # "(missing_token) Missing access token parameter"
            # Well, let's just get it working manually then.
            body = {'client_id': config[CLIENT_ID_FIELD],
                    'client_secret': config[CLIENT_SECRET_FIELD],
                    'code': code,
                    'redirect_uri': config[REDIRECT_URL_FIELD],
                    'grant_type': 'authorization_code',
                    'authorization_response': authorization_response,
            }
            access_token, refresh_token = get_token_response(body)
            connection_success = (access_token is not None)and(refresh_token is not None)
        except Exception as e:
            logging.warning("Token fetch failed: %s", str(e))
            # TODO prevent infinite loop here? Prompt after error?
            # Limit the number of retries? Parametrize?
    return access_token, refresh_token


def save_tokens(access_token, refresh_token):
    with open(TOKEN_FILENAME,"wt") as f:
        f.write(access_token+"\n"+refresh_token)
    logging.info("Saved tokens")


def get_tokens(config):
    access_token = None
    refresh_token = None
    if os.path.isfile(TOKEN_FILENAME):
        with open(TOKEN_FILENAME,"rt") as f:
            s = f.read().split('\n')
            if len(s) == 2:
                access_token, refresh_token = s[0], s[1]
                logging.info("Access token from file: %s", access_token)
                logging.info("Refresh token from file: %s",refresh_token)
                access_token, refresh_token = refresh_tokens(config, access_token, refresh_token)

    if access_token is None or refresh_token is None:
        access_token, refresh_token = get_tokens_from_scratch(config)

    logging.info("Obtained tokens successfully")
    logging.info("Final access token: %s", access_token)
    logging.info("Final refresh token: %s",refresh_token)
    return access_token, refresh_token


def generic_get_and_backup(access_token: str, parameter_name: str,
        default_fields: list, optional_fields: list = [],
        filename: str=None, readable_table_name: str=None,
        url_additions: dict={}, start_from=0, return_json: bool=False):
    result_df = pd.DataFrame(columns=default_fields+optional_fields)
    readable_table_name = \
        readable_table_name if readable_table_name is not None else parameter_name
    url = API_URL_PREFIX + parameter_name + GET_URL_POSTFIX
    try:
        # TODO consider parameters: after=1234567890&f=xml
        data = {'access_token': access_token}
        if len(optional_fields)>0:
            data['fields'] = ",".join(optional_fields)
        for i in url_additions:
            data[i] = url_additions[i]
        response = requests.post(url, data = data)
        if response.status_code == 200:
            result_json_parsed = json.loads(response.text)
            if type(result_json_parsed) == list:
                if len(result_json_parsed) > start_from:
                    result_df = pd.DataFrame(result_json_parsed[start_from:])  # 0 is num and total
                    logging.info("Read %s successfully", readable_table_name)
                else:
                    logging.info("List of %s is empty", readable_table_name)
            else:
                logging.warning("Failed to read %s. Response body: %s",
                        readable_table_name, result_json_parsed)
        else:
            logging.warning(
                "Failed to read %s. Response status code: %d.\n Detailed response: %s",
                readable_table_name, response.status_code, str(response.text))
    except Exception as e:
        logging.warning("Failed to list %s: %s", readable_table_name, str(e))

    if filename is not None:
        try:
            result_df.to_csv(filename, index=False)
            logging.info("Saved %s successfully", readable_table_name)
        except Exception as e:
            logging.warning("Failed to backup %s: %s", readable_table_name, str(e))
    else:
        logging.info("No filename provided. Not saving %s.", readable_table_name)
    if return_json:
        return result_df, result_json_parsed
    return result_df


def get_raw_tasks(access_token):
    """
    Raw tasks contain some fields in human-unreadable form. For example, folder or context.
    """
    return generic_get_and_backup(access_token=access_token, parameter_name='tasks',
        default_fields=DEFAULT_TASK_FIELDS, optional_fields=OPTIONAL_TASK_FIELDS,
        readable_table_name="raw tasks", start_from=1)


def get_and_backup_folders(access_token, filename):
    return generic_get_and_backup(access_token=access_token, filename=filename,
            parameter_name='folders', default_fields=DEFAULT_FOLDER_FIELDS)


def get_and_backup_contexts(access_token, filename):
    return generic_get_and_backup(access_token=access_token, filename=filename,
        parameter_name='contexts', default_fields=DEFAULT_CONTEXT_FIELDS)


def get_and_backup_goals(access_token, filename):
    return generic_get_and_backup(access_token=access_token, filename=filename,
        parameter_name='goals', default_fields=DEFAULT_GOAL_FIELDS)


def get_and_backup_locations(access_token, filename):
    return generic_get_and_backup(access_token=access_token, filename=filename,
        parameter_name='locations', default_fields=DEFAULT_LOCATION_FIELDS)


def get_and_backup_notes(access_token, filename):
    return generic_get_and_backup(access_token=access_token, filename=filename,
        parameter_name='notes', default_fields=DEFAULT_NOTES_FIELDS)


def backup_list_details(access_token, list_info, lists_path):
    list_col_df = pd.DataFrame(list_info["cols"])
    try:
        list_col_df.to_csv(lists_path+"cols_list_"+str(list_info["id"])+".csv", index=False)
        logging.info("Saved list %s columns successfully", list_info["id"])
    except Exception as e:
        logging.warning("Failed to backup list %s columns: %s", list_info["id"], str(e))
    #http://api.toodledo.com/3/rows/get.php?access_token=yourtoken&after=1234567890&list=1234567890
    list_row_df, row_json =generic_get_and_backup(
        access_token=access_token,
        parameter_name='rows',
        default_fields=LIST_ROW_DEFAULT_FIELDS,
        filename=lists_path+"rows_list_"+str(list_info["id"])+".csv",
        url_additions={"list": list_info["id"]},
        return_json=True)
    row_ids = list()
    col_ids = list()
    values = list()
    if len(list_row_df) > 0:
        for i in range(len(row_json)):
            for j in range(len(row_json[i]["cells"])):
                if ("c"+str(j+1)) in row_json[i]["cells"]:
                    values.append(row_json[i]["cells"]["c"+str(j+1)])
                else:
                    values.append(None)
                col_ids.append(list_info["cols"][j]["id"])
                row_ids.append(row_json[i]["id"])
        list_cell_df = pd.DataFrame({"value": values, "row_id": row_ids, "column_ids": col_ids})
    else:
        list_cell_df = pd.DataFrame({"value": [], "row_id": [], "column_ids": []})
    list_cell_df["list_id"] = list_info["id"]
    list_row_df["list_id"] = list_info["id"]
    list_col_df["list_id"] = list_info["id"]
    return list_row_df, list_col_df, list_cell_df


def get_and_backup_lists(access_token, backup_path):
    result_df = pd.DataFrame(columns=["id","added","modified","title","version","note","keywords","rows"])
    url = API_URL_PREFIX + "lists" + GET_URL_POSTFIX
    all_list_rows = None
    all_list_cols = None
    all_list_cells = None
    try:
        # TODO consider parameters: after=1234567890&f=xml
        data = {'access_token': access_token}
        response = requests.post(url, data = data)
        if response.status_code == 200:
            result_json_parsed = json.loads(response.text)
            lists_path = backup_path+"Lists"+os.path.sep
            if not os.path.isdir(lists_path):
                logging.info("Lists directory did not exist. Creating...")
                os.mkdir(lists_path)
            if type(result_json_parsed) == list:
                if len(result_json_parsed) > 0:
                    for i in result_json_parsed:
                        cur_list_rows, cur_list_cols, cur_list_cells = backup_list_details(access_token, i, lists_path)
                        if all_list_rows is None:
                            all_list_rows = cur_list_rows
                        else:
                            all_list_rows = all_list_rows.append(cur_list_rows, ignore_index=True)
                        if all_list_cols is None:
                            all_list_cols = cur_list_cols
                        else:
                            all_list_cols = all_list_cols.append(cur_list_cols, ignore_index=True)
                        if all_list_cells is None:
                            all_list_cells = cur_list_cells
                        else:
                            all_list_cells = all_list_cells.append(cur_list_cells, ignore_index=True)
                        del i["cols"]
                    result_df = pd.DataFrame(result_json_parsed)
                    logging.info("Read lists successfully")
                else:
                    logging.info("List of lists is empty")
            else:
                logging.warning("Failed to read lists. Response body: %s", result_json_parsed)
        else:
            logging.warning(
                "Failed to read lists. Response status code: %d.\n Detailed response: %s",
                response.status_code, str(response.text))
    except Exception as e:
        logging.warning("Failed to lists lists: %s", str(e))

    try:
        result_df.to_csv(backup_path+'lists.csv', index=False)
        logging.info("Saved lists successfully")
    except Exception as e:
        logging.warning("Failed to backup lists: %s", str(e))

    try:
        all_list_rows.to_csv(backup_path+'lists_rows.csv', index=False)
        logging.info("Saved all list rows successfully")
    except Exception as e:
        logging.warning("Failed to backup list rows: %s", str(e))

    try:
        all_list_cols.to_csv(backup_path+'lists_cols.csv', index=False)
        logging.info("Saved all list columns successfully")
    except Exception as e:
        logging.warning("Failed to backup list columns: %s", str(e))

    try:
        all_list_cells.to_csv(backup_path+'lists_cells.csv', index=False)
        logging.info("Saved all list cells successfully")
    except Exception as e:
        logging.warning("Failed to backup list cells: %s", str(e))

    return result_df, all_list_rows, all_list_cols


def get_and_backup_outlines(access_token, backup_path):
    all_outline_rows = None
    result_df = pd.DataFrame(columns=["id","added","modified","title","hidden","version","note","keywords","count", "updated_at"])
    url = API_URL_PREFIX + "outlines" + GET_URL_POSTFIX
    try:
        # TODO consider parameters: after=1234567890&f=xml
        data = {'access_token': access_token}
        response = requests.post(url, data = data)
        if response.status_code == 200:
            result_json_parsed = json.loads(response.text)
            outlines_path = backup_path+"Outlines"+os.path.sep
            if not os.path.isdir(outlines_path):
                logging.info("Outlines directory did not exist. Creating...")
                os.mkdir(outlines_path)
            if type(result_json_parsed) == list:
                if len(result_json_parsed) > 0:
                    for i in result_json_parsed:
                        try:
                            cur_outline_df = pd.DataFrame(i["outline"]["children"])
                            if all_outline_rows is None:
                                all_outline_rows = cur_outline_df
                            else:
                                all_outline_rows = all_outline_rows.append(cur_outline_df, ignore_index=True)
                            pd.DataFrame(cur_outline_df.to_csv(outlines_path+"outline_"+str(i["id"])+".csv", index=False))
                            logging.info("Saved outline %s successfully", i["id"])
                        except Exception as e:
                            logging.warning("Failed to backup outline %s: %s", i["id"], str(e))
                        i["count"] = i["outline"]["count"]
                        i["updated_at"] = i["outline"]["updated_at"]
                        del i["outline"]
                    result_df = pd.DataFrame(result_json_parsed)
                    logging.info("Read outlines successfully")
                else:
                    logging.info("List of outlines is empty")
            else:
                logging.warning("Failed to read outlines. Response body: %s", result_json_parsed)
        else:
            logging.warning(
                "Failed to read outlines. Response status code: %d.\n Detailed response: %s",
                response.status_code, str(response.text))
    except Exception as e:
        logging.warning("Failed to list outlines: %s", str(e))

    try:
        result_df.to_csv(backup_path+'outlines.csv', index=False)
        logging.info("Saved outlines successfully")
    except Exception as e:
        logging.warning("Failed to backup outlines: %s", str(e))

    try:
        all_outline_rows.to_csv(backup_path+'outlines_rows.csv', index=False)
        logging.info("Saved outlines rows successfully")
    except Exception as e:
        logging.warning("Failed to backup outlines rows: %s", str(e))

    return result_df, all_outline_rows


if __name__=="__main__":
    logging.basicConfig(format='%(asctime)s-%(levelname)s-%(message)s', level=logging.INFO)
    # TODO setup logging properly. Log levels, templates, maybe file.
    with open(CONFIG_FILENAME,"rt") as f:
        config = yaml.load(f, Loader=yaml.CLoader)

    access_token, refresh_token = get_tokens(config)
    save_tokens(access_token, refresh_token)
    # TODO need folders and contexts to lookup and present in readable format?
    backup_path = os.path.normpath(("" if os.path.isabs(config[BACKUP_FOLDER_FIELD]) else CUR_FILE_DIR)
            + config[BACKUP_FOLDER_FIELD]) + os.path.sep
    logging.info("Path for the backups: %s", backup_path)
    raw_tasks_df = get_raw_tasks(access_token=access_token)
    # TODO merge tasks to readable form and export them
    folders_df = get_and_backup_folders(access_token=access_token, filename=backup_path+'folders.csv')
    contexts_df = get_and_backup_contexts(access_token=access_token, filename=backup_path+'contexts.csv')
    goals_df = get_and_backup_goals(access_token=access_token, filename=backup_path+'goals.csv')
    locations_df = get_and_backup_locations(access_token=access_token, filename=backup_path+'locations.csv')
    notes_df = get_and_backup_notes(access_token=access_token, filename=backup_path+'notes.csv')
    # TODO Re-save notes one-by-one?
    lists_df, all_list_rows, all_list_cols = get_and_backup_lists(access_token=access_token, backup_path=backup_path)
    outline_df, all_outline_rows = get_and_backup_outlines(access_token=access_token, backup_path=backup_path)
    # With this databases you can merge a lot of things.
    readable_tasks_df = raw_tasks_df
    logging.info("Started the merge of tasks. Shape: %s", str(readable_tasks_df.shape))
    readable_tasks_df = pd.merge(readable_tasks_df,
        contexts_df.rename({"name": "context_name","id": "context_id"},axis=1)[["context_id","context_name"]],
        how="left", left_on="context", right_on="context_id")
    logging.info("Merged context. Shape: %s", str(readable_tasks_df.shape))
    readable_tasks_df = pd.merge(readable_tasks_df,
        locations_df.rename({"name": "location_name","id": "location_id"},axis=1)[["location_id","location_name"]],
        how="left", left_on="location", right_on="location_id")
    logging.info("Merged locations. Shape: %s", str(readable_tasks_df.shape))
    readable_tasks_df = pd.merge(readable_tasks_df,
        folders_df.rename({"name": "folder_name","id": "folder_id"},axis=1)[["folder_id","folder_name"]],
        how="left", left_on="folder", right_on="folder_id")
    logging.info("Merged folders. Shape: %s", str(readable_tasks_df.shape))
    readable_tasks_df = pd.merge(readable_tasks_df,
        folders_df.rename({"name": "goal_name","id": "goal_id"},axis=1)[["goal_id","goal_name"]],
        how="left", left_on="goal", right_on="goal_id")
    logging.info("Merged goals. Shape: %s", str(readable_tasks_df.shape))
    readable_tasks_df = readable_tasks_df.drop(["context_id","folder_id","location_id","goal_id"],axis=1)
    readable_tasks_df.to_csv(backup_path+'tasks.csv', index=False)
    logging.info("Finished writing tasks.")
    # TODO double=check that saving did not remove an element here and there

    # TODO Subfolders for lists and notes? E.g. by name prefixes
    # TODO Deleted items
    # TODO All cells of all lists? along with row ID and column ID. Later - in progress
    # TODO Some tests (at least manual) to be sure? "Back and forth" (save, load, compare)?
    # TODO Do not refresh tokens right away, make togglable
    # For each row go through each cell.
