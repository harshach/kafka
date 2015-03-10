/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  *    http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package kafka.utils

import sun.security.x509._
import java.security._
import java.security.GeneralSecurityException
import java.security.Key
import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.KeyStore
import java.security.NoSuchAlgorithmException
import java.security.PrivateKey
import java.security.SecureRandom
import java.security.cert.Certificate
import java.security.cert.X509Certificate
import javax.net.ssl.X509TrustManager

import java.io.File
import java.io.FileOutputStream
import java.io.FileWriter
import java.io.IOException
import java.io.Writer
import java.math.BigInteger
import java.net.URL
import java.util.Date
import java.util.Properties
import scala.collection.mutable.HashMap



/**
  * SSL utility functions to help with testing
  */
object TestSSLUtils extends Logging {

  @throws(classOf[GeneralSecurityException])
  @throws(classOf[IOException])
  def generateCertificate(dn: String, pair: KeyPair, days: Int, algorithm: String): X509Certificate = {
    val privateKey = pair.getPrivate()
    val info = new X509CertInfo()
    val from = new Date()
    val to = new Date(from.getTime() + days * 86400000l)
    val interval = new CertificateValidity(from, to)
    val sn = new BigInteger(64, new SecureRandom())
    val owner = new X500Name(dn)

    info.set(X509CertInfo.VALIDITY, interval)
    info.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(sn))
    info.set(X509CertInfo.SUBJECT, new CertificateSubjectName(owner))
    info.set(X509CertInfo.ISSUER, new CertificateIssuerName(owner))
    info.set(X509CertInfo.KEY, new CertificateX509Key(pair.getPublic()))
    info.set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V3))
    var algo = new AlgorithmId(AlgorithmId.md5WithRSAEncryption_oid)
    info.set(X509CertInfo.ALGORITHM_ID, new CertificateAlgorithmId(algo))

    //sign the cert to identify the algorithm that's used.
    var cert = new X509CertImpl(info)
    cert.sign(privateKey, algorithm)

    //update the algorithm, and resign.
    algo = cert.get(X509CertImpl.SIG_ALG).asInstanceOf[AlgorithmId]
    info.set(CertificateAlgorithmId.NAME + "." + CertificateAlgorithmId.ALGORITHM, algo)
    cert = new X509CertImpl(info)
    cert.sign(privateKey, algorithm)
    cert
  }

  def generateKeyPair(algorithm: String): KeyPair = {
    val keyGen = KeyPairGenerator.getInstance(algorithm)
    keyGen.initialize(1024)
    keyGen.genKeyPair
  }

  @throws(classOf[GeneralSecurityException])
  @throws(classOf[IOException])
  def createEmptyKeyStore: KeyStore = {
    val ks = KeyStore.getInstance("JKS")
    ks.load(null, null)
    ks
  }

  @throws(classOf[GeneralSecurityException])
  @throws(classOf[IOException])
  private def saveKeyStore(ks: KeyStore, filename: String, password: String) {
    val out = new FileOutputStream(filename)
    try {
      ks.store(out, password.toCharArray)
    } finally {
      out.close()
    }
  }

  @throws(classOf[GeneralSecurityException])
  @throws(classOf[IOException])
  def createKeyStore(filename: String, password: String, keyPassword: String, alias: String,
                     privateKey: Key, cert: Certificate) {
    val ks = createEmptyKeyStore
    ks.setKeyEntry(alias, privateKey, keyPassword.toCharArray, Array(cert))
    saveKeyStore(ks, filename, password)
  }

  @throws(classOf[GeneralSecurityException])
  @throws(classOf[IOException])
  def createTrustStore(filename: String, password: String, certs: HashMap[String, X509Certificate]) {
    val ks = createEmptyKeyStore
    certs foreach {case (key, cert) => ks.setCertificateEntry(key, cert)}
    saveKeyStore(ks, filename, password)
  }

  def trustAllCerts: X509TrustManager = {
    val trustManager = new X509TrustManager() {
      override def getAcceptedIssuers: Array[X509Certificate] = {
        null
      }
      override def checkClientTrusted(certs: Array[X509Certificate], authType: String) {
      }
      override def checkServerTrusted(certs: Array[X509Certificate], authType: String) {
      }
    }
    trustManager
  }

  /**
    * Creates a temporary ssl config file
    */
  def tempSslConfigFile(): File = {
    val certs =  new HashMap[String, X509Certificate]
    val keyPair = generateKeyPair("RSA")
    val cert = generateCertificate("CN=localhost, O=localhost", keyPair, 30, "SHA1withRSA")
    val password = "test"

    val keyStoreFile = File.createTempFile("keystore", ".jks")
    createKeyStore(keyStoreFile.getPath(), password, password, "localhost", keyPair.getPrivate, cert)
    certs.put("localhost", cert)
    val trustStoreFile = File.createTempFile("truststore", ".jks")
    createTrustStore(trustStoreFile.getPath(), password, certs)

    val f = File.createTempFile("ssl.server",".properties")
    val sslProps = new Properties
    sslProps.put("port", TestUtils.choosePort().toString)
    sslProps.put("host", "localhost")
    sslProps.put("keystore.type", "jks")
    sslProps.put("want.client.auth", "false")
    sslProps.put("need.client.auth", "false")
    sslProps.put("keystore", keyStoreFile.getPath)
    sslProps.put("keystorePwd", password)
    sslProps.put("keyPwd", password)
    sslProps.put("truststore", trustStoreFile.getPath)
    sslProps.put("truststorePwd", password)
    val outputStream = new FileOutputStream(f)
    sslProps.store(outputStream, "")
    outputStream.flush()
    outputStream.getFD().sync()
    outputStream.close()
    f.deleteOnExit()
    f
  }

}
