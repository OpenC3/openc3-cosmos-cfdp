/*
# Copyright 2026 OpenC3, Inc.
# All Rights Reserved.
#
# Licensed for Evaluation and Educational Use
#
# This file may only be used commercially under the terms of a commercial license
# purchased from OpenC3, Inc.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
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
const pluginGem = 'openc3-cosmos-cfdp-0.0.0.gem'

async function openFile(page, utils, filename) {
  await page.locator('[data-test=script-runner-file]').click()
  await page.locator('text=Open File').click()
  await utils.sleep(500) // Allow background data to fetch
  await expect(
    page.locator('.v-dialog').getByText('CFDP', { exact: true }),
  ).toBeVisible()
  await utils.sleep(500)
  let parts = filename.split('.')
  await page.locator('[data-test=file-open-save-search] input').fill(parts[0])
  await utils.sleep(500)
  await page
    .locator('[data-test=file-open-save-search] input')
    .fill(`.${parts[1]}`)
  await expect(page.locator(`text=${filename}`).first()).toBeVisible()
  await page.locator(`text=${filename}`).first().click()
  await page.locator('[data-test="file-open-save-submit-btn"]').click()
  await expect(page.locator('.v-dialog')).not.toBeVisible()

  // Check for potential "<User> is editing this script"
  // This can happen if we had to do a retry on this test
  const someone = page.getByText('is editing this script')
  if (await someone.isVisible()) {
    await page.locator('[data-test="unlock-button"]').click()
    await page.locator('[data-test="confirm-dialog-force unlock"]').click()
  }
}

test('installs the CFDP plugin', async ({ page, utils }) => {
  await page.goto('/tools/admin/plugins')

  // Wait for the plugin list to load before checking if already installed
  await expect(page.locator('[data-test=plugin-list]')).toBeVisible()
  const pluginListItem = page.locator('[data-test=plugin-list-item]', {
    hasText: plugin,
  })
  if (await pluginListItem.isVisible()) {
    return // Plugin already installed (probably either local or a retry in GH actions)
  }

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
  await page.getByRole('combobox', { name: 'plugin_test_mode' }).click()
  await page.getByRole('option', { name: 'TRUE' }).click()
  await page.locator('data-test=edit-submit').click()
  await expect(page.locator('[data-test=plugin-alert]')).toContainText(
    'Started installing',
  )
  // Check for Complete
  let regexp = new RegExp(`Processing plugin_install: ${pluginGem} - Complete`)
  await expect(page.locator('[data-test=process-list]')).toContainText(regexp, {
    timeout: 30000,
  })

  await expect(
    page.locator(`[data-test=plugin-list] div:has-text("${plugin}")`).first(),
  ).toContainText('CFDP')

  await utils.sleep(10000) // Allow the plugin microservices to start
})

test('runs the CFDP test suite', async ({ page, utils }) => {
  await openFile(page, utils, 'cfdp_test_suite.rb')

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
  await openFile(page, utils, 'cfdp_test_suite.rb')

  // Run group setup so medium.bin exists
  await page.locator('[data-test="setup-group"]').click()
  await expect(page.locator('.v-dialog')).toContainText('Script Results', {
    timeout: 10000,
  })
  let textarea = await page.inputValue('.v-dialog >> textarea')
  expect(textarea).toMatch('Pass: 1')
  await page.keyboard.press('Escape')

  await openFile(page, utils, 'interrupt_test.rb')

  // Start the script
  await page.locator('[data-test="clear-log"]').click()
  await page.locator('[data-test="confirm-dialog-clear"]').click()
  await page.locator('[data-test="start-button"]').click()
  await expect(page.locator('[data-test=state] input')).toHaveValue(/running/, {
    timeout: 20000,
  })
  await utils.sleep(1000)

  // In another tab, restart the CfdpUser microservices (both send and receive side)
  const pageTwo = await context.newPage()
  pageTwo.goto('/tools/admin/microservices', {
    waitUntil: 'domcontentloaded',
  })
  await pageTwo
    .locator('.v-list-item')
    .filter({ hasText: 'DEFAULT__USER__CFDP' })
    .first()
    .getByRole('button', { name: 'Restart Microservice' })
    .click()
  await pageTwo.locator('[data-test="confirm-dialog-restart"]').click()
  await utils.sleep(1000)
  await pageTwo
    .locator('.v-list-item')
    .filter({ hasText: 'DEFAULT__USER__CFDP2' })
    .first()
    .getByRole('button', { name: 'Restart Microservice' })
    .click()
  await pageTwo.locator('[data-test="confirm-dialog-restart"]').click()

  await expect(page.locator('[data-test=output-messages]')).toContainText(
    'Script completed: CFDP/procedures/interrupt_test.rb',
    {
      timeout: 600000, // 10min }
    },
  )

  // Clean up by running suite teardown
  await openFile(page, utils, 'cfdp_test_suite.rb')

  // Run group teardown to delete test files
  await page.locator('[data-test="teardown-group"]').click()
  await expect(page.locator('.v-dialog')).toContainText('Script Results', {
    timeout: 10000,
  })
  textarea = await page.inputValue('.v-dialog >> textarea')
  expect(textarea).toMatch('Pass: 1')
})
