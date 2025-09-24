/*
# Copyright 2025 OpenC3, Inc.
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
  toolPath: '/tools/scriptrunner',
  toolName: 'Script Runner',
  storageState: 'storageState.json',
})

test.describe.configure({ mode: 'serial' })
test.setTimeout(1800000) // 30min

const plugin = 'openc3-cosmos-cfdp'
const pluginGem = 'openc3-cosmos-cfdp-1.0.0.gem'

test('installs the CFDP plugin', async ({ page }) => {
  await page.goto('/tools/admin/plugins')

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
})

test('runs the CFDP test suite', async ({ page, utils }) => {
  await page.locator('[data-test=script-runner-file]').click()
  await page.locator('text=Open File').click()
  await utils.sleep(500) // Allow background data to fetch
  await expect(
    page.locator('.v-dialog').getByText('CFDP', { exact: true })
  ).toBeVisible()
  await page.locator('[data-test=file-open-save-search] input').fill('cfdp_')
  await utils.sleep(100)
  await page.locator('[data-test=file-open-save-search] input').fill('test_')
  await utils.sleep(100)
  await page.locator('[data-test=file-open-save-search] input').fill('suite')
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

test('continues transaction after microservice restart', async ({
  page,
  utils,
  context,
}) => {
  await page.goto('/tools/scriptrunner')

  // Open the suite (we're just gonna run the group setup, though)
  await page.locator('[data-test=script-runner-file]').click()
  await page.locator('text=Open File').click()
  await utils.sleep(500) // Allow background data to fetch
  await expect(
    page.locator('.v-dialog').getByText('CFDP', { exact: true })
  ).toBeVisible()
  await page.locator('[data-test=file-open-save-search] input').fill('cfdp_')
  await utils.sleep(100)
  await page.locator('[data-test=file-open-save-search] input').fill('test_')
  await utils.sleep(100)
  await page.locator('[data-test=file-open-save-search] input').fill('suite')
  await utils.sleep(100)
  await page.getByText('cfdp_test_suite.rb').first().click()
  await page.locator('[data-test="file-open-save-submit-btn"]').click()
  await expect(page.locator('.v-dialog')).not.toBeVisible()

  // Check for potential "<User> is editing this script"
  // This can happen if we had to do a retry on this test
  let someone = page.getByText('is editing this script')
  if (await someone.isVisible()) {
    await page.locator('[data-test="unlock-button"]').click()
    await page.locator('[data-test="confirm-dialog-force unlock"]').click()
  }

  // Run group setup so medium.bin exists
  await page.locator('[data-test="setup-group"]').click()
  await expect(page.locator('.v-dialog')).toContainText('Script Results', {
    timeout: 10000,
  })
  let textarea = await page.inputValue('.v-dialog >> textarea')
  expect(textarea).toMatch('Pass: 1')
  await page.keyboard.press('Escape')

  // Open the actual test script
  await page.locator('[data-test=script-runner-file]').click()
  await page.locator('text=Open File').click()
  await utils.sleep(500) // Allow background data to fetch
  await expect(
    page.locator('.v-dialog').getByText('CFDP', { exact: true })
  ).toBeVisible()
  await page
    .locator('[data-test=file-open-save-search] input')
    .fill('interrupt_')
  await utils.sleep(100)
  await page.locator('[data-test=file-open-save-search] input').fill('test')
  await utils.sleep(100)
  await page.getByText('interrupt_test.rb').first().click()
  await page.locator('[data-test="file-open-save-submit-btn"]').click()
  await expect(page.locator('.v-dialog')).not.toBeVisible()

  // Check for potential "<User> is editing this script"
  // This can happen if we had to do a retry on this test
  someone = page.getByText('is editing this script')
  if (await someone.isVisible()) {
    await page.locator('[data-test="unlock-button"]').click()
    await page.locator('[data-test="confirm-dialog-force unlock"]').click()
  }

  // Start the script
  await page.locator('[data-test="clear-log"]').click()
  await page.locator('[data-test="confirm-dialog-clear"]').click()
  await page.locator('[data-test="start-button"]').click()

  // In another tab, restart the CfdpUser microservice
  const pageTwo = await context.newPage()
  pageTwo.goto('/tools/admin/microservices', {
    waitUntil: 'domcontentloaded',
  })
  await pageTwo
    .locator('.v-list-item')
    .filter({ hasText: 'DEFAULT__USER__CFDP' })
    .first()
    .locator('.mdi-play')
    .click()
  await pageTwo.locator('[data-test="confirm-dialog-start"]').click()

  await expect(page.locator('[data-test=output-messages]')).toContainText(
    'Script completed: CFDP/procedures/interrupt_test.rb',
    {
      timeout: 600000, // 10min }
    }
  )
})
