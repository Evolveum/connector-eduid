/*
 * Copyright (c) 2019 Evolveum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.evolveum.polygon.connector.eduid;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.AbstractFilterTranslator;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;

/**
 * Created by oscar on 25. 3. 2019.
 */
public class EduIdFilterTranslator extends AbstractFilterTranslator<EduIdFilter> {
    private static final Log LOG = Log.getLog(EduIdFilterTranslator.class);

    @Override
    protected EduIdFilter createEqualsExpression(EqualsFilter filter, boolean not) {
        LOG.ok("createEqualsExpression, filter: {0}, not: {1}", filter, not);

        if (not) {
            return null;            // not supported
        }

        Attribute attr = filter.getAttribute();
        LOG.ok("attr.getName:  {0}, attr.getValue: {1}", attr.getName(), attr.getValue());
        if (Uid.NAME.equals(attr.getName()) || Name.NAME.equals(attr.getName())) {
            if (attr.getValue() != null && attr.getValue().get(0) != null) {
                EduIdFilter eduIdFilter = new EduIdFilter();
                eduIdFilter.byUid = String.valueOf(attr.getValue().get(0));
                LOG.ok("EduIdFilter: {0}", eduIdFilter);
                return eduIdFilter;
            }
        }
        return null;            // not supported
    }
}
