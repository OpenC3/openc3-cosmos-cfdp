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
  storageState: 'storageState.json',
})

let plugin = 'openc3-cosmos-cfdp'
let pluginGem = 'openc3-cosmos-cfdp-1.0.0.gem'

test.setTimeout(1800000) // 30min
test('installs a new plugin', async ({ page, utils }) => {
  // Note that Promise.all prevents a race condition
  // between clicking and waiting for the file chooser.
  const [fileChooser] = await Promise.all([
    // It is important to call waitForEvent before click to set up waiting.
    page.waitForEvent('filechooser'),
    // Opens the file chooser.
    await page.getByRole('button', { name: 'Install From File' }).click(),
  ])
  await fileChooser.setFiles(`../${pluginGem}`)
  await expect(page.locator('.v-dialog:has-text("Variables")')).toBeVisible()
  await page.getByLabel('plugin_test_mode', { exact: true }).dblclick()
  await page.getByLabel('plugin_test_mode', { exact: true }).fill('true')
  await page.locator('data-test=edit-submit').click()
  await expect(page.locator('[data-test=plugin-alert]')).toContainText(
    'Started installing'
  )
  // Plugin install can go so fast we can't count on 'Running' to be present so try catch this
  let regexp = new RegExp(`Processing plugin_install: .* - Running`)
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
    page.locator(`[data-test=plugin-list] div:has-text("${plugin}")`).first()
  ).toContainText('CFDP')

  // Run ScriptRunner Test
  await page.goto('/tools/scriptrunner')

  await page.locator('[data-test=script-runner-file]').click()
  await page.locator('text=Open File').click()
  await utils.sleep(500) // Allow background data to fetch
  await expect(
    page.locator('.v-dialog').getByText('CFDP', { exact: true })
  ).toBeVisible()
  await page
    .locator('[data-test=file-open-save-search] input')
    .fill('cfdp_')
  await utils.sleep(100)
  await page
    .locator('[data-test=file-open-save-search] input')
    .fill('test_')
  await utils.sleep(100)
  await page
    .locator('[data-test=file-open-save-search] input')
    .fill('suite')
  await utils.sleep(100)
  await page.getByText('cfdp_test_suite.rb').first().click()
  await page.locator('[data-test="file-open-save-submit-btn"]').click()
  await expect(page.locator('.v-dialog')).not.toBeVisible()

  // Check for potential "<User> is editing this script"
  // This can happen if we had to do a retry on this test
  const someone = page.getByText('is editing this script')
  if (await someone.isVisible()) {
    await page.locator('[data-test="unlock-button"]').click()
    await page.locator('[data-test="confirm-dialog-force unlock"]').click()
  }

  await page.locator('[data-test="start-suite"]').click()
  // Wait for the results ... allow for additional time
  await expect(page.locator('.v-dialog')).toContainText('Script Results', {
    timeout: 1200000, // 20min
  })
  let textarea = await page.inputValue('.v-dialog >> textarea')
  expect(textarea).toMatch('Pass: 4')
})
