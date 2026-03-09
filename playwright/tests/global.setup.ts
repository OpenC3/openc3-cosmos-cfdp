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

import { test as setup } from '@playwright/test'
import { STORAGE_STATE } from './../playwright.config'

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

  // Save signed-in state to 'storageState.json'
  await page.context().storageState({ path: STORAGE_STATE })

  // On the initial load you might get the Clock out of sync dialog
  if (await page.getByText('Clock out of sync').isVisible()) {
    await page.locator("text=Don't show this again").click()
    await page.locator('button:has-text("Dismiss")').click()
  }
})
