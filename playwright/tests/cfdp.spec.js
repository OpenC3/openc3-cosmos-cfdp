/*
# Copyright 2022 OpenC3, Inc.
# All Rights Reserved.
#
# This program is free software; you can modify and/or redistribute it
# under the terms of the GNU Affero General Public License
# as published by the Free Software Foundation; version 3 with
# attribution addendums as found in the LICENSE.txt
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
*/

// @ts-check
import { test, expect } from './fixture'

test.use({
  toolPath: '/tools/admin/plugins',
  toolName: 'Administrator',
  storageState: 'adminStorageState.json',
})

let plugin = 'openc3-cosmos-cfdp'
let pluginGem = 'openc3-cosmos-cfdp-1.0.0.gem'

test.setTimeout(300000)
test('installs a new plugin', async ({ page, utils }) => {
  // Note that Promise.all prevents a race condition
  // between clicking and waiting for the file chooser.
  const [fileChooser] = await Promise.all([
    // It is important to call waitForEvent before click to set up waiting.
    page.waitForEvent('filechooser'),
    // Opens the file chooser.
    await page.locator('text=Click to select').click({ force: true }),
  ])
  await fileChooser.setFiles(`../${pluginGem}`)
  await expect(page.locator('.v-dialog:has-text("Variables")')).toBeVisible()
  await page.getByLabel('plugin_test_mode').dblclick();
  await page.getByLabel('plugin_test_mode').fill('true');
  await page.locator('data-test=edit-submit').click()
  await expect(page.locator('[data-test=plugin-alert]')).toContainText(
    'Started installing'
  )
  // Plugin install can go so fast we can't count on 'Running' to be present so try catch this
  let regexp = new RegExp(
    `Processing plugin_install: .* - Running`
  )
  try {
    await expect(page.locator('[data-test=process-list]')).toContainText(
      regexp,
      {
        timeout: 30000,
      }
    )
  } catch {}
  // Ensure no Running are left
  await expect(page.locator('[data-test=process-list]')).not.toContainText(
    regexp,
    {
      timeout: 30000,
    }
  )
  // Check for Complete
  regexp = new RegExp(`Processing plugin_install: ${pluginGem} - Complete`)
  await expect(page.locator('[data-test=process-list]')).toContainText(regexp)

  await expect(
    page.locator(
      `[data-test=plugin-list] div[role=listitem]:has-text("${plugin}")`
    )
  ).toContainText('CFDP')
  // Show the process output
  await page
    .locator(
      `[data-test=process-list] div[role=listitem]:has-text("${plugin}") >> [data-test=show-output]`
    )
    .first()
    .click()
  await expect(page.locator('.v-dialog--active')).toContainText(
    'Process Output'
  )
  await expect(page.locator('.v-dialog--active')).toContainText(
    `Loading new plugin: ${pluginGem}`
  )
  await page.locator('.v-dialog--active >> button:has-text("Ok")').click()

  // Run ScriptRunner Test
  await page.goto('/tools/scriptrunner')

  await page.locator('[data-test="cosmos-script-runner-file"]').click();
  await page.getByText('Open File').click();
  await page.getByRole('button', { name: '󰍝' }).first().click();
  await page.getByRole('button', { name: '󰍝' }).nth(2).click();
  await page.getByText('cfdp_test_suite.rb').click();
  await page.locator('[data-test="file-open-save-submit-btn"]').click();
  await page.locator('[data-test="start-suite"]').click();
  let dialog = page.locator('.v-dialog.v-dialog--active')
  await dialog.waitFor({timeout: 300000})
  await expect(dialog).toContainText(
    'Script Results',
    {
      timeout: 300000,
    }
  )
  let textarea = await page.inputValue('.v-dialog >> textarea')
  expect(textarea).toMatch('Pass: 4')
})
