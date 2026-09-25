"""Offline regression tests. No real PBS, MQTT or sendmail delivery is performed."""
import contextlib
from copy import deepcopy
import importlib.util
import io
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'pbs-datastore-sync-verify,prune-gc.py'
# Make the sibling package importable when unittest starts outside the project.
sys.path.insert(0, str(SCRIPT.parent))
from pbs_maintenance import settings, maintenance, logging_config, mail, mqtt as mqtt_client

spec = importlib.util.spec_from_file_location('pbs_cli', SCRIPT)
app = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = app
spec.loader.exec_module(app)


def write_toml(path, config):
    """Serialize these simple test fixtures without a production TOML writer dependency."""
    lines = []
    for section, values in config.items():
        lines.append('[' + section + ']')
        for key, value in values.items():
            lines.append(key + ' = ' + json.dumps(value))
    path.write_text('\n'.join(lines) + '\n')


class MaintenanceTests(unittest.TestCase):
    def setUp(self):
        """Give every test a private script/config/log directory."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.config = deepcopy(settings.DEFAULT_CONFIG)
        self.config['jobs'].update(sync_job='sync-test', verify_job='verify-test', gc_datastore='store-test')
        self.config['prune']['job'] = 'prune-test'
        self.config['dry_run']['enabled'] = False
        self.config['mqtt'].update(host='mqtt.invalid', topic='test/status')
        self.config['email'].update(from_address='pbs@example.com',
                                    to_addresses=['admin@example.com'])

    def run_app(self, config=None, fail_step=None, mqtt_error=False, email_error=False, launch_error=False, raw=None):
        """Run actual config loading/validation/logging with only external effects mocked."""
        config = self.config if config is None else config
        path = self.folder / 'config.toml'
        if raw is None:
            write_toml(path, config)
        else:
            path.write_text(raw)
        commands, mqtt_events, email_events = [], [], []

        def run(argv, **kwargs):
            commands.append(argv)
            if launch_error:
                raise FileNotFoundError('test missing command')
            return maintenance.CmdResult(argv, 7 if argv[1] == fail_step else 0,
                                 'output tail', 'error tail' if argv[1] == fail_step else '')

        def mqtt_send(**kwargs):
            mqtt_events.append(kwargs)
            if mqtt_error:
                raise RuntimeError('test broker unavailable')

        def email_send(email_settings, sendmail_settings, payload, logger):
            email_events.append(deepcopy(payload))
            if email_error:
                raise RuntimeError('test mail unavailable')

        with patch.object(app, 'SCRIPT_DIR', self.folder), \
             patch.object(logging_config, 'SCRIPT_DIR', self.folder), \
             patch.object(sys, 'argv', [str(SCRIPT)]), \
             patch.object(mqtt_client, 'mqtt', object()), \
             patch.object(maintenance, 'run_cmd_stream', side_effect=run), \
             patch.object(mqtt_client, 'mqtt_publish', side_effect=mqtt_send), \
             patch.object(mail, 'email_send', side_effect=email_send), \
             patch.object(mail, 'find_sendmail', return_value='/usr/sbin/sendmail'), \
             patch.object(app.shutil, 'which', return_value='/test/proxmox-backup-manager') as which, \
             patch.object(maintenance.subprocess, 'Popen', side_effect=AssertionError('No child allowed in mocked workflow')), \
             contextlib.redirect_stderr(io.StringIO()):
            rc = app.main()
            if config['dry_run']['enabled']:
                which.assert_not_called()
        return rc, commands, mqtt_events, email_events

    def test_fixed_order_and_success(self):
        self.config['mqtt']['on_success'] = True
        rc, commands, events, mail = self.run_app()
        self.assertEqual(rc, 0)
        self.assertEqual(commands, [
            ['proxmox-backup-manager', 'sync-job', 'run', 'sync-test'],
            ['proxmox-backup-manager', 'verify-job', 'run', 'verify-test'],
            ['proxmox-backup-manager', 'prune-job', 'run', 'prune-test'],
            ['proxmox-backup-manager', 'garbage-collection', 'start', 'store-test']])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]['payload']['event'], 'pbs_maintenance_success')
        self.assertFalse(events[0]['payload']['dry_run'])
        self.assertEqual(mail, [])
        self.assertEqual(list((self.folder/'logs').glob('*.err')), [])
        self.assertEqual(len(list((self.folder/'logs').glob('*.log'))), 1)

    def test_each_failure_stops_later_commands(self):
        for index, (command, step) in enumerate([('sync-job', 'sync'), ('verify-job', 'verify'),
                                               ('prune-job', 'prune'), ('garbage-collection', 'gc')]):
            with self.subTest(step=step):
                rc, commands, events, _ = self.run_app(fail_step=command)
                self.assertEqual(rc, 1)
                self.assertEqual(len(commands), index + 1)
                payload = events[0]['payload']
                self.assertEqual(payload['failed_step'], step)
                self.assertEqual(payload['returncode'], 7)
                self.assertEqual(payload['stderr_tail'], 'error tail')
                self.assertTrue(Path(payload['err_file']).exists())

    def test_validation_has_no_external_side_effects(self):
        cases = [('jobs', 'sync_job', ''), ('jobs', 'verify_job', ''), ('jobs', 'gc_datastore', ''),
                 ('prune', 'job', ''), ('prune', 'mode', 'unknown'), ('mqtt', 'port', 0),
                 ('mqtt', 'port', True), ('mqtt', 'max_output_chars', 0), ('mqtt', 'timeout_sec', -1),
                 ('mqtt', 'host', ''), ('mqtt', 'topic', 'bad/#'), ('prune', 'keep_last', -1),
                 ('prune', 'keep_last', True), ('steps', 'sync', 'false'), ('logging', 'typo', True),
                 ('email', 'to_addresses', [False]), ('sendmail', 'path', 123)]
        for section, key, value in cases:
            with self.subTest(section=section, key=key, value=value):
                cfg = deepcopy(self.config); cfg[section][key] = value
                self.assertEqual(self.run_app(cfg), (2, [], [], []))
        cfg = deepcopy(self.config); cfg['steps'] = dict(sync=False, verify=False, prune=False, gc=False)
        self.assertEqual(self.run_app(cfg), (2, [], [], []))

    def test_manual_retention_forwarding_and_payload(self):
        self.config['steps'] = dict(sync=False, verify=False, prune=True, gc=False)
        self.config['prune'].update(mode='manual', job='', datastore='manual-store')
        # No retention, and an ambiguous job/manual selection, must both fail closed.
        self.assertEqual(self.run_app(), (2, [], [], []))
        values = dict(last=7, daily=14, weekly=8, monthly=12, yearly=3)
        expected = ['proxmox-backup-manager', 'prune', 'run', 'manual-store']
        for name, count in values.items():
            self.config['prune']['keep_'+name] = count
            expected += ['--keep-'+name, str(count)]
        self.config['prune']['job'] = 'ambiguous'
        self.assertEqual(self.run_app(), (2, [], [], []))
        self.config['prune']['job'] = ''
        self.config['mqtt']['on_success'] = True
        rc, commands, events, _ = self.run_app()
        self.assertEqual((rc, commands), (0, [expected]))
        self.assertEqual(events[0]['payload']['prune_keep'], values)
        self.assertEqual(events[0]['payload']['prune_mode'], 'manual')
        self.assertIsNone(events[0]['payload']['datastore'])

    def test_single_enabled_step_and_disabled_payload_fields(self):
        self.config['steps'] = dict(sync=False, verify=True, prune=False, gc=False)
        self.config['mqtt']['on_success'] = True
        rc, commands, events, _ = self.run_app()
        self.assertEqual(rc, 0)
        self.assertEqual(commands, [['proxmox-backup-manager', 'verify-job', 'run', 'verify-test']])
        payload = events[0]['payload']
        for key in ('sync_job', 'prune_mode', 'prune_job', 'prune_datastore', 'prune_keep', 'datastore'):
            self.assertIsNone(payload[key])

    def test_notification_failures_do_not_suppress_other_channel(self):
        self.config['mqtt']['on_success'] = True
        self.config['email'].update(enabled=True, on_success=True)
        for mqtt_error, email_error, failed_step in [(True, False, None), (False, True, None),
                                                   (True, True, 'verify-job')]:
            with self.subTest(mqtt=mqtt_error, email=email_error, step=failed_step):
                rc, commands, events, mail = self.run_app(fail_step=failed_step,
                                                         mqtt_error=mqtt_error, email_error=email_error)
                self.assertEqual(rc, 1)
                self.assertEqual((len(events), len(mail)), (1, 1))
                self.assertEqual(len(commands), 2 if failed_step else 4)

    def test_success_notifications_are_explicit_opt_ins(self):
        # Master MQTT is enabled by default, but success is silent until opted in.
        rc, commands, events, mail_events = self.run_app()
        self.assertEqual((rc, len(commands), events, mail_events), (0, 4, [], []))

        self.config['mqtt']['on_success'] = True
        rc, _, events, mail_events = self.run_app()
        self.assertEqual((rc, len(events), len(mail_events)), (0, 1, 0))
        self.assertEqual(events[0]['payload']['event'], 'pbs_maintenance_success')

        self.config['mqtt']['on_success'] = False
        self.config['email'].update(enabled=True, on_success=True)
        rc, _, events, mail_events = self.run_app()
        self.assertEqual((rc, len(events), len(mail_events)), (0, 0, 1))
        self.assertEqual(mail_events[0]['event'], 'pbs_maintenance_success')

    def test_failure_notifications_ignore_success_opt_out(self):
        self.config['mqtt']['on_success'] = False
        self.config['email'].update(enabled=True, on_success=False)
        rc, commands, events, mail_events = self.run_app(fail_step='verify-job')
        self.assertEqual((rc, len(commands), len(events), len(mail_events)), (1, 2, 1, 1))
        self.assertEqual(events[0]['payload']['event'], 'pbs_maintenance_failed')
        self.assertEqual(mail_events[0]['event'], 'pbs_maintenance_failed')

    def test_failure_tail_limit(self):
        self.config['mqtt']['max_output_chars'] = 4
        rc, _, events, _ = self.run_app(fail_step='sync-job')
        self.assertEqual(rc, 1)
        self.assertEqual(events[0]['payload']['stdout_tail'], 'tail')
        self.assertEqual(events[0]['payload']['stderr_tail'], 'tail')

    def test_dry_run_is_silent_by_default_and_never_executes(self):
        self.config['dry_run']['enabled'] = True
        self.config['email']['enabled'] = True
        self.config['mqtt']['host'] = ''
        self.assertEqual(self.run_app(), (0, [], [], []))
        log = next((self.folder/'logs').glob('*.log')).read_text()
        self.assertIn('DRY RUN [sync]', log)
        self.assertIn('prune-job run prune-test', log)
        self.assertEqual(list((self.folder/'logs').glob('*.err')), [])

    def test_dry_run_channels_are_independent_explicit_opt_ins(self):
        self.config['mqtt'].update(enabled=False, retain=True)
        self.config['email']['enabled'] = False
        for mqtt_enabled, email_enabled in [(False, False), (True, False), (False, True), (True, True)]:
            with self.subTest(mqtt=mqtt_enabled, email=email_enabled):
                self.config['dry_run'].update(enabled=True, send_mqtt=mqtt_enabled, send_email=email_enabled)
                rc, commands, events, mail = self.run_app()
                self.assertEqual((rc, commands), (0, []))
                self.assertEqual(len(events), int(mqtt_enabled))
                self.assertEqual(len(mail), int(email_enabled))
                for payload in [e['payload'] for e in events] + mail:
                    self.assertEqual(payload['event'], 'pbs_maintenance_dry_run')
                    self.assertTrue(payload['dry_run'])
                    self.assertEqual(len(payload['commands']), 4)
                if events:
                    self.assertFalse(events[0]['retain'])

    def test_dry_run_notification_errors_and_missing_settings(self):
        self.config['dry_run'].update(enabled=True, send_email=True, send_mqtt=True)
        rc, commands, events, mail = self.run_app(mqtt_error=True)
        self.assertEqual((rc, commands, len(events), len(mail)), (1, [], 1, 1))
        self.config['email']['to_addresses'] = []
        self.assertEqual(self.run_app(), (2, [], [], []))

    def test_real_notifications_can_both_be_disabled(self):
        self.config['mqtt']['enabled'] = False
        rc, commands, events, mail = self.run_app()
        self.assertEqual((rc, len(commands), events, mail), (0, 4, [], []))

    def test_launch_failure_is_logged_and_notified(self):
        rc, commands, events, _ = self.run_app(launch_error=True)
        self.assertEqual((rc, len(commands)), (1, 1))
        self.assertEqual(events[0]['payload']['returncode'], 127)
        self.assertTrue(Path(events[0]['payload']['err_file']).exists())

    def test_help_and_version_require_no_configuration(self):
        cases = [
            ('--help', ('-c, --config PATH', 'operational settings are not CLI flags',
                        'relative paths resolve from the current working directory',
                        'Logs still go beside the launcher')),
            ('--version', ('0.0.7',)),
        ]
        for flag, expected_texts in cases:
            with self.subTest(flag=flag), patch.object(sys, 'argv', [str(SCRIPT), flag]), \
                 patch.object(logging_config, 'build_logger') as log, patch.object(settings, 'load_config') as load, \
                 contextlib.redirect_stdout(io.StringIO()) as output:
                with self.assertRaises(SystemExit) as result:
                    app.main()
                self.assertEqual(result.exception.code, 0)
                normalized_output = " ".join(output.getvalue().split())
                for expected in expected_texts:
                    self.assertIn(expected, normalized_output)
                log.assert_not_called(); load.assert_not_called()

    def test_local_stream_capture_and_error_only_file(self):
        child = self.folder/'child.py'
        child.write_text('import sys; print("ordinary stdout"); print("progress 50%", file=sys.stderr); '
                         'print("WARNING: slow", file=sys.stderr); print("0 errors"); '
                         'print("TASK ERROR: real failure", file=sys.stderr); sys.exit(7)')
        with contextlib.redirect_stdout(io.StringIO()) as out, contextlib.redirect_stderr(io.StringIO()) as err:
            logger = logging_config.build_logger(True, self.folder/'logs')
            try:
                result = maintenance.run_cmd_stream([sys.executable, str(child)], env=None, logger=logger)
                logger.warning('ordinary warning')
                full_path, err_path = logger.log_file, logger.err_file
            finally:
                logging_config.close_logger(logger)
        self.assertEqual(result.returncode, 7)
        self.assertIn('ordinary stdout', result.stdout)
        self.assertIn('progress 50%', result.stderr)
        self.assertEqual(out.getvalue().count('ordinary stdout'), 1)
        self.assertEqual(err.getvalue().count('progress 50%'), 1)
        full, errors = full_path.read_text(), err_path.read_text()
        for text in ('ordinary stdout', 'progress 50%', 'WARNING: slow', '0 errors', 'ordinary warning'):
            self.assertIn(text, full); self.assertNotIn(text, errors)
        self.assertIn('TASK ERROR: real failure', errors)
        self.assertIn('FAILED (rc=7)', errors)
        self.assertEqual(full_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(err_path.stat().st_mode & 0o777, 0o600)

    def test_stderr_progress_alone_does_not_create_err(self):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            logger = logging_config.build_logger(False, self.folder/'logs')
            try:
                maintenance.run_cmd_stream([sys.executable, '-c', 'import sys; print("progress", file=sys.stderr)'],
                                   env=None, logger=logger)
                err_path = logger.err_file
            finally:
                logging_config.close_logger(logger)
        self.assertFalse(err_path.exists())

    def test_config_errors_are_logged_without_toml_source(self):
        self.assertEqual(self.run_app(raw='password = "secret-with-unclosed-string'), (2, [], [], []))
        full = next((self.folder/'logs').glob('*.log')).read_text()
        self.assertNotIn('secret-with-unclosed-string', full)
        self.assertIn('Configuration/preflight error', full)
        self.assertEqual(len(list((self.folder/'logs').glob('*.err'))), 1)
        self.assertEqual(self.run_app(raw='[unknown]\nx = true'), (2, [], [], []))

    def test_bundled_config_covers_defaults_and_is_safe(self):
        bundled = settings.load_config(SCRIPT.parent/'config-example.toml')
        self.assertEqual(bundled, settings.DEFAULT_CONFIG)
        self.assertTrue(bundled['dry_run']['enabled'])
        self.assertEqual(settings.notification_channels(bundled), dict(mqtt=False, email=False))
        live = deepcopy(bundled); live['dry_run']['enabled'] = False
        self.assertEqual(settings.notification_channels(live, 'pbs_maintenance_success'), dict(mqtt=False, email=False))
        self.assertEqual(settings.notification_channels(live, 'pbs_maintenance_failed'), dict(mqtt=True, email=False))
        with (SCRIPT.parent/'config-example.toml').open('rb') as stream:
            self.assertEqual(settings.tomllib.load(stream), settings.DEFAULT_CONFIG)

    def test_config_relative_file_paths(self):
        self.config['mqtt']['cafile'] = 'mqtt.pem'
        self.config['sendmail']['path'] = 'bin/sendmail'
        path = self.folder/'custom.toml'; write_toml(path, self.config)
        loaded = settings.load_config(path)
        self.assertEqual(loaded['mqtt']['cafile'], str(self.folder/'mqtt.pem'))
        self.assertEqual(loaded['sendmail']['path'], str(self.folder/'bin/sendmail'))

    def test_script_local_paths_ignore_caller_working_directory(self):
        # main has no --config argument in run_app; SCRIPT_DIR is deliberately a
        # different path from cwd, proving default config/log anchoring together.
        self.assertNotEqual(self.folder, Path.cwd())
        self.assertEqual(self.run_app()[0], 0)
        self.assertTrue((self.folder/'logs').is_dir())

    def test_log_creation_failure_prevents_work(self):
        (self.folder/'logs').write_text('a file, not a directory')
        self.assertEqual(self.run_app(), (2, [], [], []))

    def test_missing_paho_prevents_real_work_but_not_silent_dry_run(self):
        args = settings.config_to_args(self.config)
        with patch.object(mqtt_client, 'mqtt', None):
            with self.assertRaisesRegex(ValueError, 'paho-mqtt'):
                settings.validate_config(self.config, args)
            self.config['dry_run']['enabled'] = True
            settings.validate_config(self.config, args)

    def test_missing_toml_parser_is_actionable(self):
        with patch.object(settings, 'tomllib', None):
            with self.assertRaisesRegex(ValueError, 'tomli'):
                settings.load_config(self.folder/'config.toml')

    def test_sendmail_message_and_command(self):
        email_settings = deepcopy(self.config['email'])
        sendmail_settings = {'path': '/custom/sendmail'}
        payload = dict(dry_run=True, event='pbs_maintenance_dry_run', hostname='pbs-test')
        logger = logging.getLogger('test')
        completed = subprocess.CompletedProcess(['/custom/sendmail', '-t'], 0, b'', b'')
        with patch.object(mail, 'find_sendmail', return_value='/custom/sendmail') as finder, \
             patch.object(mail.subprocess, 'run', return_value=completed) as run:
            mail.email_send(email_settings, sendmail_settings, payload, logger)
        finder.assert_called_once_with('/custom/sendmail')
        run.assert_called_once()
        argv = run.call_args.args[0]
        self.assertEqual(argv, ['/custom/sendmail', '-t'])
        self.assertTrue(run.call_args.kwargs['check'])
        self.assertTrue(run.call_args.kwargs['capture_output'])
        message_bytes = run.call_args.kwargs['input']
        text = message_bytes.decode(errors='replace')
        self.assertIn('Subject: [PBS maintenance] DRY RUN - pbs-test', text)
        self.assertIn('From: pbs@example.com', text)
        self.assertIn('To: admin@example.com', text)
        self.assertIn('pbs_maintenance_dry_run', text)

    def test_sendmail_nonzero_exit_is_an_error(self):
        failure = subprocess.CalledProcessError(75, ['/usr/sbin/sendmail', '-t'], stderr=b'queue unavailable')
        with patch.object(mail, 'find_sendmail', return_value='/usr/sbin/sendmail'), \
             patch.object(mail.subprocess, 'run', side_effect=failure):
            with self.assertRaisesRegex(RuntimeError, 'status 75'):
                mail.email_send(self.config['email'], {'path': ''},
                                dict(dry_run=False, event='pbs_maintenance_success', hostname='pbs'),
                                logging.getLogger('test'))

    def test_find_sendmail_override_standard_paths_and_path(self):
        with tempfile.TemporaryDirectory() as folder:
            explicit = Path(folder) / 'sendmail'
            explicit.write_text('#!/bin/sh\nexit 0\n')
            explicit.chmod(0o700)
            self.assertEqual(mail.find_sendmail(str(explicit)), str(explicit))
            self.assertIsNone(mail.find_sendmail(str(Path(folder) / 'missing')))
        with patch.object(Path, 'is_file', return_value=False), patch.object(mail.shutil, 'which', return_value='/path/sendmail'):
            self.assertEqual(mail.find_sendmail(''), '/path/sendmail')

    def test_active_email_requires_sendmail(self):
        self.config['mqtt']['enabled'] = False
        self.config['email']['enabled'] = True
        with patch.object(mail, 'find_sendmail', return_value=None):
            args = settings.config_to_args(self.config)
            with self.assertRaisesRegex(ValueError, 'requires sendmail'):
                settings.validate_config(self.config, args)

    def test_active_email_header_validation(self):
        self.config['email']['enabled'] = True
        for key, value in [('from_address', 'bad\n@example.com'),
                           ('to_addresses', ['bad\r@example.com']), ('subject_prefix', 'bad\nheader')]:
            with self.subTest(key=key):
                config = deepcopy(self.config); config['email'][key] = value
                self.assertEqual(self.run_app(config), (2, [], [], []))

    def test_error_classifier_does_not_match_routine_mentions(self):
        for line in ('0 errors', 'no errors found', 'WARNING: error count is zero', 'verify failed count: 0', 'progress'):
            self.assertFalse(logging_config.is_error_line(line), line)
        for line in ('Error: broken', 'TASK ERROR: broken', '[ERROR] broken', 'FATAL: broken',
                     '2026-09-24T12:00:00Z: TASK ERROR: broken'):
            self.assertTrue(logging_config.is_error_line(line), line)


    def prepare_cli_fixture(self):
        """Copy the runtime and substitute a harmless PBS executable for CLI tests."""
        runtime = self.folder / 'runtime'
        runtime.mkdir()
        shutil.copy2(SCRIPT, runtime / SCRIPT.name)
        shutil.copytree(SCRIPT.parent / 'pbs_maintenance', runtime / 'pbs_maintenance',
                        ignore=shutil.ignore_patterns('__pycache__'))
        caller = self.folder / 'caller'
        caller.mkdir()
        binaries = self.folder / 'bin'
        binaries.mkdir()
        fake = binaries / 'proxmox-backup-manager'
        fake.write_text(
            '#!' + sys.executable + '\n'
            'import json, os, sys\n'
            'with open(os.environ["PBS_TEST_TRACE"], "a") as f: f.write(json.dumps(sys.argv[1:])+"\\n")\n'
            'print("routine progress", file=sys.stderr)\n'
            'if sys.argv[1] == os.environ.get("PBS_TEST_FAIL"):\n'
            '    print("TASK ERROR: simulated failure", file=sys.stderr)\n'
            '    sys.exit(7)\n')
        fake.chmod(0o700)
        env = os.environ.copy()
        env.update(PATH=str(binaries), PYTHONDONTWRITEBYTECODE='1',
                   PBS_TEST_TRACE=str(self.folder / 'command-trace.jsonl'))
        return runtime, caller, env

    def test_cli_dry_run_from_other_directory_and_symlink(self):
        """Check real package imports and root-relative config/log paths via both launch paths."""
        runtime, caller, env = self.prepare_cli_fixture()
        self.config['dry_run']['enabled'] = True
        write_toml(runtime / 'config.toml', self.config)
        link = caller / 'maintenance.py'
        link.symlink_to(runtime / SCRIPT.name)
        for launcher in (runtime / SCRIPT.name, link):
            result = subprocess.run([sys.executable, '-B', str(launcher)], cwd=caller,
                                    env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('DRY RUN [sync]', result.stderr)
        self.assertFalse(Path(env['PBS_TEST_TRACE']).exists())
        self.assertEqual(len(list((runtime / 'logs').glob('*.log'))), 2)
        self.assertFalse(list((runtime / 'logs').glob('*.err')))
        self.assertFalse((caller / 'logs').exists())
        self.assertFalse((runtime / 'pbs_maintenance' / 'logs').exists())

    def test_cli_simulated_commands_cross_module_boundaries(self):
        """Run the real CLI against a fake executable to check success, stop order and file logs."""
        runtime, caller, env = self.prepare_cli_fixture()
        self.config['mqtt']['enabled'] = False
        self.config['email']['enabled'] = False
        config = caller / 'site.toml'
        write_toml(config, self.config)
        trace = Path(env['PBS_TEST_TRACE'])
        for fail_step, config_flag, expected in [
                ('', '-c', ['sync-job', 'verify-job', 'prune-job', 'garbage-collection']),
                ('verify-job', '--config', ['sync-job', 'verify-job'])]:
            with self.subTest(fail_step=fail_step, config_flag=config_flag):
                if trace.exists():
                    trace.unlink()
                env['PBS_TEST_FAIL'] = fail_step
                result = subprocess.run([sys.executable, '-B', str(runtime / SCRIPT.name),
                                         config_flag, 'site.toml'], cwd=caller, env=env,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 1 if fail_step else 0, result.stderr)
                commands = [json.loads(line)[0] for line in trace.read_text().splitlines()]
                self.assertEqual(commands, expected)
                errors = list((runtime / 'logs').glob('*.err'))
                self.assertEqual(len(errors), int(bool(fail_step)))
                if errors:
                    text = errors[0].read_text()
                    self.assertIn('TASK ERROR: simulated failure', text)
                    self.assertNotIn('routine progress', text)
        self.assertFalse((caller / 'logs').exists())

    def test_package_imports_have_no_runtime_side_effects(self):
        """Import every module in a fresh process with process/network creation forbidden."""
        runtime, caller, env = self.prepare_cli_fixture()
        code = (
            'import sys, socket, subprocess; from unittest.mock import patch; '
            'sys.path.insert(0, sys.argv[1])\n'
            'with patch.object(subprocess, "Popen", side_effect=AssertionError("child")), '
            'patch.object(socket.socket, "connect", side_effect=AssertionError("network")):\n'
            '    from pbs_maintenance import settings, maintenance, logging_config, mail, mqtt\n'
        )
        result = subprocess.run([sys.executable, '-B', '-c', code, str(runtime)],
                                cwd=caller, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse((runtime / 'logs').exists())
        self.assertFalse(Path(env['PBS_TEST_TRACE']).exists())


if __name__ == '__main__':
    unittest.main()
