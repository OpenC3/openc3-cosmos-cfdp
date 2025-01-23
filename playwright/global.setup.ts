/*
# Copyright 2024 OpenC3, Inc.
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

import { test as setup, expect } from '@playwright/test'
import { STORAGE_STATE } from './playwright.config'

setup('global setup', async ({ page }) => {
  await page.goto('/tools/cmdtlmserver')
  // Wait for the nav bar to populate
  for (let i = 0; i < 10; i++) {
    await page
      .locator('nav:has-text("CmdTlmServer")')
      .waitFor({ timeout: 30000 })
    // If we don't see CmdTlmServer then refresh the page
    if (!(await page.$('nav:has-text("CmdTlmServer")'))) {
      await page.reload()
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
  }
  if (await page.getByText('Enter the password').isVisible()) {
    await page.getByLabel('Password').fill('password')
    await page.locator('button:has-text("Login")').click()
  } else {
    await page.getByLabel('New Password').fill('password')
    await page.getByLabel('Confirm Password').fill('password')
    await page.click('data-test=set-password')
  }
  await new Promise((resolve) => setTimeout(resolve, 500))

  // Save signed-in state to 'storageState.json' and adminStorageState to match Enterprise
  await page.context().storageState({ path: STORAGE_STATE })

  // On the initial load you might get the Clock out of sync dialog
  if (await page.getByText('Clock out of sync').isVisible()) {
    await page.locator("text=Don't show this again").click()
    await page.locator('button:has-text("Dismiss")').click()
  }
})
