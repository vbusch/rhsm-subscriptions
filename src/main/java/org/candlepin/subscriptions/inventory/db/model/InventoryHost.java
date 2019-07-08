/*
 * Copyright (c) 2009 - 2019 Red Hat, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 * Red Hat trademarks are not licensed under GPLv3. No permission is
 * granted to use or replicate Red Hat trademarks that are incorporated
 * in this software or its documentation.
 */
package org.candlepin.subscriptions.inventory.db.model;

import com.vladmihalcea.hibernate.type.json.JsonBinaryType;

import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Table;


/**
 * Represents a host entity stored in the inventory service's database.
 */
@Entity
@Table(name = "hosts")
@TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)
public class InventoryHost implements Serializable {

    @Id
    private UUID id;

    private String account;

    @Column(name = "display_name")
    private String displayName;

    @Column(name = "ansible_host")
    private String ansibleHost;

    @Column(name = "created_on")
    private OffsetDateTime createdOn;

    @Column(name = "modified_on")
    private OffsetDateTime modifiedOn;

    @SuppressWarnings("squid:S1948")
    @Type(type = "jsonb")
    @Column(columnDefinition = "jsonb")
    @Basic(fetch = FetchType.LAZY)
    private Map<String, Map<String, Object>> facts;

    @SuppressWarnings("squid:S1948")
    @Type(type = "jsonb")
    @Column(name = "canonical_facts", columnDefinition = "jsonb")
    @Basic(fetch = FetchType.LAZY)
    private Map<String, Object> canonicalFacts;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getAnsibleHost() {
        return ansibleHost;
    }

    public void setAnsibleHost(String ansibleHost) {
        this.ansibleHost = ansibleHost;
    }

    public OffsetDateTime getCreatedOn() {
        return createdOn;
    }

    public void setCreatedOn(OffsetDateTime createdOn) {
        this.createdOn = createdOn;
    }

    public OffsetDateTime getModifiedOn() {
        return modifiedOn;
    }

    public void setModifiedOn(OffsetDateTime modifiedOn) {
        this.modifiedOn = modifiedOn;
    }

    public Map<String, Map<String, Object>> getFacts() {
        return facts;
    }

    public void setFacts(Map<String, Map<String, Object>> facts) {
        this.facts = facts;
    }

    public Map<String, Object> getCanonicalFacts() {
        return canonicalFacts;
    }

    public void setCanonicalFacts(Map<String, Object> canonicalFacts) {
        this.canonicalFacts = canonicalFacts;
    }
}
