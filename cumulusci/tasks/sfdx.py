""" Wrapper tasks for the SFDX CLI


TODO: Instead of everyone overriding random attrs, especially as future
users subclass these tasks, we should expose an api for the string format
function. i.e. make it easy for subclasses to add to the string inherited
from the base.

Actually do this in Command. have it expose command segments.

Then here in SFDX we will add an additional metalayer for
how the CLI formats args opts commands etc.
"""
import json

from cumulusci.core.config import ScratchOrgConfig
from cumulusci.core.exceptions import CommandException
from cumulusci.tasks.command import Command

SFDX_CLI = 'sfdx'


class SFDXBaseTask(Command):
    """ Call the sfdx cli with params and no org """

    command = 'force --help'

    task_options = {
        'command': {
            'description': 'The full command to run with the sfdx cli.',
            'required': True,
        },
        'extra': {
            'description': 'Append additional options to the command',
        },
    }

    def _init_options(self, kwargs):
        super(SFDXBaseTask, self)._init_options(kwargs)
        self.options['command'] = self._get_command()

    def _get_command(self):
        command = '{SFDX_CLI} {command}'.format(
            command=self.options.get('command', self.command),
            SFDX_CLI=SFDX_CLI
        )
        return command


class SFDXOrgTask(SFDXBaseTask):
    """ Call the sfdx cli with a workspace username """

    salesforce_task = True

    def _init_options(self, kwargs):
        super(SFDXOrgTask, self)._init_options(kwargs)

        # Add username to command if needed  
        self.options['command'] = self._add_username(self.options['command'])  

        # Add extra command args from
        if self.options.get('extra'):
            self.options['command'] += ' {}'.format(self.options['extra'])

        self.logger.info('Running command:  {}'.format(self.options['command']))

    def _get_command(self):
        command = super(SFDXOrgTask, self)._get_command()
        # For scratch orgs, just pass the username in the command line
        if isinstance(self.org_config, ScratchOrgConfig):
            command += ' -u {username}'.format(
                username=self.org_config.username,
            )
        return command

    def _get_env(self):
        env = super(SFDXOrgTask, self)._get_env()
        if not isinstance(self.org_config, ScratchOrgConfig):
            # For non-scratch keychain orgs, pass the access token via env var
            env['SFDX_INSTANCE_URL'] = self.org_config.instance_url
            env['SFDX_USERNAME'] = self.org_config.access_token
        return env

class SFDXJsonTask(SFDXBaseTask):

    def _process_output(self, line):
        try:
            self.return_values['data'] = json.loads(line)
        except:
            self.logger.error('Failed to parse json from line: {}'.format(line))
            raise
        
        self._process_data()

    def _process_data(self):
        self.logger.info('JSON = {}'.format(self.return_values['data']))


class SFDXJsonPollingTask(SFDXJsonTask):

    def _init_task(self):
        super(SFDXJsonPollingTask, self)._init_task()
        self.job_id = None

    def _process_output(self, line):
        started = False
        if hasattr(self, 'job_id'):
            started = True

        super(SFDXJsonPollingTask, self)._process_output(line)

        if not started:
            self._process_data()
        else:
            self._process_poll_data()

    def _process_data(self):
        if self.job_id:
            return self._process_poll_data() 

        self.job_id = self.return_values['data']['id']
        self._poll()

    def _process_poll_data(self):
        self.logger.info(self.return_values['data'])
        if self._check_poll_done(self.return_values['data']):
            self.poll_complete = True

    def _poll_action(self):
        command = self._get_poll_command()
        env = self._get_env()
        self._run_command(
            env,
            command = command,
        )
        

    def _check_poll_done(self):
        return self.return_values['data'].get('done', True)

    def _process_poll_output(self, line):
        pass

    def _get_poll_command(self):
        raise NotImplementedError(
            'Subclassess should provide an implementation'
        )

class SFDXConvertFrom(SFDXJsonTask):
    """ Use sfdx force:source:convert to convert from MDAPI to sfdx format """

    command = 'force:source:convert'
    
    task_options = {
        'src': {
            'description': 'The path of the Salesforce DX format source to be converted.  Default: force-app',
        },
        'dest': {
            'description': 'The path to write the converted metadata to.  Default: src',
        }
    }
    
    def _init_options(self, kwargs):
        self.options.setdefault('src', 'force-app')
        self.options.setdefault('dest', 'src')
        self.options['command'] = self.command
        super(SFDXConvertFrom, self)._init_options(kwargs)

    def _get_command(self):
        command = super(SFDXConvertFrom, self)._get_command()
        command = '{} -r {} -d {} --json'.format(command, self.options['src'], self.options['dest'])
        return command

    def _process_data(self):
        status = self.return_values['data'].get('status')
        if status == 0:
            self.logger.info('Successfully converted DX format from {src} to {dest}'.format(**self.options))

    def _handle_returncode(self, returncode, stderr):
        if returncode:
            self.logger.error(self.return_values['data']['message'])
            raise CommandException(self.return_values['data']['message'])

class SFDXConvertTo(SFDXJsonTask):
    """ Use sfdx force:mdapi:convert to convert from MDAPI to sfdx format """

    command = 'force:mdapi:convert'
    
    task_options = {
        'src': {
            'description': 'The path of the metadata source to be converted.  Default: src',
        },
        'dest': {
            'description': 'The path to write the converted metadata to.  Default: force-app',
        }
    }
    
    def _init_options(self, kwargs):
        self.options.setdefault('src', 'src')
        self.options.setdefault('dest', 'force-app')
        self.options['command'] = self.command
        super(SFDXConvertTo, self)._init_options(kwargs)

    def _get_command(self):
        command = super(SFDXConvertTo, self)._get_command()
        command = '{} -r {} -d {} --json'.format(command, self.options['src'], self.options['dest'])
        return command

    def _process_data(self):
        if self.return_values['data'].get('status') == 0:
            self.logger.info('Successfully converted metadata from {src} to {dest}'.format(**self.options)) 

class SFDXDeploy(SFDXJsonPollingTask, SFDXOrgTask):
    """ Use sfdx force:mdapi:deploy to deploy a local directory of metadata """

    task_options = {
        'path': {
            'description': 'The path of the metadata to be deployed.',
            'required': True,
        },
    }

    def _get_command(self):
        command = super(SFDXDeploy, self)._get_command()
        if hasattr(self, 'options'):
            command += ' -d {}'.format(self.options.get('path', 'NO_PATH_PROVIDED'))
        return command

    def _get_poll_command(self):
        if not self.job_id:
            return None
        command = super(SFDXDeploy, self)._get_command()
        command += ' -i {}'.format(self.job_id)
        return command

    def _init_options(self, kwargs):
        super(SFDXDeploy, self)._init_options(kwargs)
        # Rewrite the command with the path merged in
        self.options['command'] = self._get_command()
