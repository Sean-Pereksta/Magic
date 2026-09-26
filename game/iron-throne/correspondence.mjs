import { ART } from './asset-manifest.mjs';
import { CAMPAIGN_HOUSES } from './data.mjs';
import { installCorrespondence } from './correspondence-ui.mjs';

installCorrespondence(document, { portraits: ART.portraits, houses: CAMPAIGN_HOUSES });
