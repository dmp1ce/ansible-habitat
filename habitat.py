#!/usr/bin/python
# -*- coding: utf-8 -*-

from ansible.module_utils.basic import AnsibleModule
import requests
import tempfile
import os
import time

def is_habitat_supervisor_running(module):
    cmd = "%s sup status" % (HABITAT_PATH)
    rc, stdout, stderr = module.run_command(cmd, check_rc=False)

    # Habitat documented return codes:
    # https://www.habitat.sh/docs/run-packages-monitoring/
    if rc == 3:
        return False
    elif rc == 0:
        return True
    else:
        module.fail_json(msg="Unknown error with 'hab sup status'",
                         rc=rc, stdout=stdout, stderr=stderr)

def turn_off_supervisor(module, exit):
    cmd = "%s sup term" % (HABITAT_PATH)
    rc, stdout, stderr = module.run_command(cmd, check_rc=True)
 
    if exit:
        module.exit_json(changed=True, msg='Terminated Habitat supervisor',
                         rc=rc, stdout=stdout, stderr=stderr)
    else:
        return True

def turn_on_supervisor(module, exit):
    # Currently there is no way, that I know of, to run
    # supervisor in background with Ansible

    #cmd = "%s sup run" % (HABITAT_PATH)
    #rc, stdout, stderr = module.run_command(cmd, check_rc=True,
    #                                        use_unsafe_shell=True)
 
    #if exit:
    #    module.exit_json(changed=True, msg='Started Habitat supervisor',
    #                     rc=rc, stdout=stdout, stderr=stderr)
    #else:
    #    return True

    # Report failure to user
    module.fail_json(msg='Starting Habitat supervisior in background is not supported')

def stop_service(module, style, exit):
    p = module.params

    if style == 'transient':
        stop_cmd = "stop"
    elif style == 'persistent':
        stop_cmd = "unload"
    else:
        module.fail_json(msg="Unknown style '%s'" % (current_style))

    cmd = "%s sup %s %s/%s" % (HABITAT_PATH, stop_cmd, p['origin'], p['name'])

    rc, stdout, stderr = module.run_command(cmd, check_rc=True)
 
    if exit:
        module.exit_json(changed=True, msg='Stopped %s/%s' %
            (p['origin'], p['name']), rc=rc, stdout=stdout, stderr=stderr)

def _start_service(module, style, exit):
    p = module.params

    if style == 'persistent':
        style_cmd = "load"
    elif style == 'transient':
        style_cmd = "start"

    cmd = "%s sup %s --no-color %s/%s" % (HABITAT_PATH, style_cmd, p['origin'], p['name'])

    rc, stdout, stderr = module.run_command(cmd, check_rc=True)
 
    if exit:
        module.exit_json(changed=True, msg="%s/%s started" % (p['origin'], p['name']),
                         rc=rc, stdout=stdout, stderr=stderr)
    else:
        return True

def start_service(module, exit):
    return _start_service(module, 'transient', exit)

def load_service(module, exit):
    return _start_service(module, 'persistent', exit)

def toggle_service_style(module, current_style, exit):
    if current_style == 'transient':
        stop_service(module, 'persistent', False)
        load_service(module, exit)
    if current_style == 'persistent':
        stop_service(module, 'persistent', False)
        start_service(module, exit)

def get_service_config(name, group):
    return requests.get('http://127.0.0.1:9631/services/%s/%s/config' %
        (name, group))

def get_next_incarnation(name, group):
    r = requests.get('http://127.0.0.1:9631/census')
    return (r.json()['census_groups']["%s.%s" % (name, group)]['service_config']['incarnation'] + 1)

def get_style(name, group):
    r = requests.get('http://127.0.0.1:9631/services/%s/%s' %
        (name, group))
    return (r.json()['start_style'].lower())

def get_state(name, group):
    r = requests.get('http://127.0.0.1:9631/services/%s/%s' %
        (name, group))
    try:
        s = r.json()['process']['state'].lower()
    except:
        s = None

    return (s)

# https://github.com/cjohnweb/python-dict-recursive-diff
def recursive_diff(data, temp_data):
    new_data = {}
    for k in data.keys():
        if type(data[k]) == type({}):
            if k not in temp_data:
                temp_data[k] = {}
            temp = recursive_diff(data[k], temp_data[k])  
            if temp:
                new_data[k] = {}
                new_data[k] = temp
        else:
            if k in temp_data:
                if data[k] != temp_data[k]:
                    new_data[k] = data[k]
            else:
                new_data[k] = data[k]
    return new_data

def update_service(module, env_update, exit):
    try:
        import toml
    except ModuleNotFoundError:
        module.fail_json(msg="toml Python library is required")

    p = module.params

    # Create TOML to apply to service
    tmp_fd, tmp_path = tempfile.mkstemp()
    f = os.fdopen(tmp_fd, 'w')
    toml.dump(env_update, f)
    f.close()

    # Get service incarnation number from census
    next_incarnation = get_next_incarnation(p['name'], p['group'])

    # Create command
    cmd = "%s config apply %s.%s %s %s" % (HABITAT_PATH, p['name'], p['group'], next_incarnation, tmp_path)

    rc, stdout, stderr = module.run_command(cmd, check_rc=True)

    if exit:
        module.exit_json(changed=True, msg="%s.%s updated" % (p['name'], p['group']),
                         rc=rc, stdout=stdout, stderr=stderr)
    else:
        return True

def process_service_config(module):
    """ Decide what to do with the environment input """
    p = module.params

    changed = False
    last_msg =  ""

    # Get current config
    state = get_state(p['name'], p['group'])
    if state == 'down' or state == None:
        # Start service so config is available
        if state == 'down':
            start_service(module, False)
        elif state == None:
            load_service(module, False)
        time.sleep(5)
        config = get_service_config(p['name'], p['group'])
        changed = True
        last_msg = "Started %s/%s" % (p['origin'], p['name'])
    else:
        config = get_service_config(p['name'], p['group'])

    if not config:
        module.fail_json(msg="Failed to get %s/%s config" % (p['name'], p['group']))

    diff = recursive_diff(p['environment'], config.json())

    # Get current start style
    style = get_style(p['name'], p['group'])

    # Determine if a change needs to be made
    if diff:
        if not style == p['style']:
            toggle_service_style(module, style, False)
            time.sleep(1)
        update_service(module, p['environment'], True)
    else:
        if not style == p['style']:
            toggle_service_style(module, style, True)
        module.exit_json(changed=changed, msg=last_msg)
    
def main():
    module = AnsibleModule(
        argument_spec   = dict(
            origin      = dict(default="core", type='str'),
            name        = dict(default=None, type='str'),
            group       = dict(default="default", type='str'),
            sup_state   = dict(default='up', choices=['up', 'down']),
            state       = dict(default='up', choices=['up', 'down']),
            style       = dict(default='persistent', choices=['persistent', 'transient']),
            environment = dict(required=False, type='dict')
        ),
        required_one_of=[['name', 'sup_state']],
        mutually_exclusive=[['name', 'sup_state']]
    )

    global HABITAT_PATH
    HABITAT_PATH = module.get_bin_path('hab', required=True)

    p = module.params

    # Verify supervisor is running
    hab_sup_up=is_habitat_supervisor_running(module)
    if hab_sup_up and p["sup_state"] == 'down':
        turn_off_supervisor(module, True)
    elif not hab_sup_up and p["sup_state"] == 'up':
        turn_on_supervisor(module, True)

    # No need to continue if service name is not specified
    if not p["name"]:
        module.exit_json()

    if p['state'] == 'up':
        process_service_config(module)
    else:
        s =  get_state(p['name'], p['group'])

        if get_state(p['name'], p['group']) == 'up':
            stop_service(module, 'transient', True)
        else:
            module.exit_json()

if __name__ == '__main__':
    main()
