// Open the user-visible options disclosure before exercising a moved action.
export async function revealChatAction(page,selector){
 const action=page.locator(selector),menu=action.locator('xpath=ancestor::details[contains(@class,"chat-options")]');
 if(await menu.count()&&!await menu.evaluate(el=>el.open))await menu.locator(':scope > summary').click();
 return action;
}
export async function clickChatAction(page,selector){await (await revealChatAction(page,selector)).click();}
